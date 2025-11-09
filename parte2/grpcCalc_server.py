import hashlib
import logging
from logging.handlers import RotatingFileHandler
import random
import threading
import time
from concurrent import futures
import grpc

import grpcCalc_pb2 as pb
import grpcCalc_pb2_grpc as pbg


# -------------------- Logging --------------------
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("MinerAPI")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)

    fh = RotatingFileHandler("server.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)

    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)
    logger.propagate = False
    return logger


LOGGER = setup_logger()


# -------------------- Interceptor p/ log de RPC --------------------
class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        handler = continuation(handler_call_details)
        if handler is None or handler.unary_unary is None:
            return handler

        unary = handler.unary_unary

        def wrapped(request, context):
            start = time.time()
            peer = context.peer()
            summary = []
            for f in ("id", "transactionID", "clientID"):
                if hasattr(request, f):
                    summary.append(f"{f}={getattr(request, f)}")
            LOGGER.info(f"[{peer}] RPC IN  | {method} | {' '.join(summary)}")
            try:
                resp = unary(request, context)
                elapsed = (time.time() - start) * 1000.0
                status_val = None
                for f in ("status", "code"):
                    if hasattr(resp, f):
                        status_val = getattr(resp, f)
                        break
                LOGGER.info(f"[{peer}] RPC OUT | {method} | status={status_val} | {elapsed:.2f}ms")
                return resp
            except Exception as e:
                elapsed = (time.time() - start) * 1000.0
                LOGGER.exception(f"[{peer}] RPC ERR | {method} | {elapsed:.2f}ms | {e}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Internal server error")
                raise

        return grpc.unary_unary_rpc_method_handler(
            wrapped,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


# -------------------- Tabela de transações (memória) --------------------
class TxTable:
    def __init__(self):
        self.lock = threading.Lock()
        self.current_id = 0
        self.table = {}
        self._new_tx(initial=True)

    def _new_tx(self, initial: bool = False):
        difficulty = random.randint(1, 7)
        with self.lock:
            if not initial:
                self.current_id += 1
            self.table[self.current_id] = {
                "id": self.current_id,
                "difficulty": difficulty,
                "solution": "",
                "winner": -1,
            }
            LOGGER.info(f"[table] nova tx criada id={self.current_id} diff={difficulty}")

    def get_tx(self, txid: int):
        with self.lock:
            return self.table.get(txid)

    def get_current_id(self) -> int:
        need_new = False
        with self.lock:
            cur = self.table[self.current_id]
            if cur["winner"] != -1:
                need_new = True
            cid = self.current_id
        if need_new:
            self._new_tx()
            with self.lock:
                cid = self.current_id
        return cid

    def resolve(self, txid: int, solution: str, client_id: int) -> int:
        need_new = False
        with self.lock:
            tx = self.table.get(txid)
            if tx is None:
                return -1
            if tx["winner"] != -1:
                return 2
            tx["solution"] = solution
            tx["winner"] = client_id
            need_new = True
        if need_new:
            self._new_tx()
        return 1


TXS = TxTable()


# -------------------- Regra de validação --------------------
def valid_solution(solution: str, difficulty: int) -> bool:
    required = max(1, min(int(difficulty), 7))
    h = hashlib.sha1(solution.encode("utf-8")).hexdigest()
    return h.startswith("0" * required)


# -------------------- Implementação das RPCs --------------------
class MinerServicer(pbg.MinerAPIServicer):
    def getTransactionID(self, request, context):
        client_id = context.peer()
        LOGGER.info(f"[{client_id}] Requisição para getTransactionID")
        resp = pb.TransactionID(id=TXS.get_current_id())
        return resp

    def getChallenge(self, request, context):
        client_id = context.peer()
        tx = TXS.get_tx(request.id)
        diff = -1 if tx is None else tx["difficulty"]
        LOGGER.info(f"[{client_id}] getChallenge id={request.id} -> diff={diff}")
        return pb.Challenge(difficulty=diff)

    def getTransactionStatus(self, request, context):
        client_id = context.peer()
        tx = TXS.get_tx(request.id)
        status = -1 if tx is None else (0 if tx["winner"] != -1 else 1)
        LOGGER.info(f"[{client_id}] getTransactionStatus id={request.id} -> status={status}")
        return pb.Status(status=status)

    def submitChallenge(self, request, context):
        client_id = context.peer()
        tx = TXS.get_tx(request.transactionID)
        if tx is None:
            LOGGER.warning(f"[{client_id}] submit invalid id tx={request.transactionID}")
            return pb.SubmitReply(code=-1)
        if tx["winner"] != -1:
            LOGGER.info(f"[{client_id}] submit already solved tx={request.transactionID}")
            return pb.SubmitReply(code=2)

        if valid_solution(request.solution, tx["difficulty"]):
            code = TXS.resolve(request.transactionID, request.solution, request.clientID)
            LOGGER.info(f"[{client_id}] submit ok tx={request.transactionID} cid={request.clientID} -> code={code}")
            return pb.SubmitReply(code=code)

        LOGGER.info(f"[{client_id}] submit invalid solution tx={request.transactionID} cid={request.clientID}")
        return pb.SubmitReply(code=0)

    def getWinner(self, request, context):
        client_id = context.peer()
        tx = TXS.get_tx(request.id)
        cid = -1 if tx is None else (0 if tx["winner"] == -1 else tx["winner"])
        LOGGER.info(f"[{client_id}] getWinner id={request.id} -> cid={cid}")
        return pb.WinnerReply(clientID=cid)

    def getSolution(self, request, context):
        client_id = context.peer()
        tx = TXS.get_tx(request.id)
        if tx is None:
            LOGGER.warning(f"[{client_id}] getSolution id={request.id} -> inválido")
            return pb.SolutionReply(status=-1, solution="", difficulty=-1)

        status = 0 if tx["winner"] != -1 else 1
        solution = tx["solution"] if tx["winner"] != -1 else ""
        diff = tx["difficulty"]

        LOGGER.info(f"[{client_id}] getSolution id={request.id} -> status={status} diff={diff} sol={solution}")
        return pb.SolutionReply(status=status, solution=solution, difficulty=diff)


# -------------------- Bootstrap do servidor --------------------
def serve(host: str = "0.0.0.0:8080", max_workers: int = 10):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers), interceptors=[LoggingInterceptor()])
    pbg.add_MinerAPIServicer_to_server(MinerServicer(), server)
    server.add_insecure_port(host)
    LOGGER.info(f"MinerAPI gRPC server iniciando em {host} ...")
    server.start()
    LOGGER.info("MinerAPI gRPC server em execução. Ctrl+C para encerrar.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        LOGGER.info("Encerrando servidor... aguardando shutdown gracioso.")
        server.stop(grace=None)
        LOGGER.info("Servidor finalizado.")


if __name__ == "__main__":
    serve()
