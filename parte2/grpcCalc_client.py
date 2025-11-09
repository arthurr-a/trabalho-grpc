import argparse
import hashlib
import os
import time
import grpc
import grpcCalc_pb2 as pb
import grpcCalc_pb2_grpc as pbg
import threading  # Importando threading para controle de threads
import queue  # Importando o módulo queue

# Função para gerar o hash SHA1
def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# Função para validar o hash
def is_valid(solution: str, difficulty: int) -> bool:
    required = max(1, int(difficulty))
    return sha1_hex(solution).startswith("0" * required)

# Função de mineração local
def mine_locally(txid: int, client_id: int, difficulty: int, threads: int | None = None):
    prefix = f"{txid}:{client_id}:"
    threads = threads or max(1, (os.cpu_count() or 2))
    stop_event = threading.Event()  # Usando threading para controle
    out_q = queue.Queue()  # Corrigido: Usando queue.Queue()

    def worker(start_nonce: int, step: int):
        nonce = start_nonce
        while not stop_event.is_set():
            s = prefix + str(nonce)
            if is_valid(s, difficulty):
                out_q.put(s)
                stop_event.set()
                return
            nonce += step

    workers = []
    for i in range(threads):
        t = threading.Thread(target=worker, args=(i, threads), daemon=True)
        t.start()
        workers.append(t)

    try:
        solution = out_q.get(timeout=3600)  # 1h máx
    except queue.Empty:
        solution = ""

    stop_event.set()
    for t in workers:
        t.join(timeout=0.1)
    return solution


# Função para exibir o menu
def print_menu():
    print("\n=== Miner CLI ===", flush=True)
    print("1 - getTransactionID")
    print("2 - getChallenge")
    print("3 - getTransactionStatus")
    print("4 - getWinner")
    print("5 - getSolution")
    print("6 - Mine (buscar solução e submeter)")
    print("0 - Sair")


# Função principal
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="localhost:8080", help="endereço host:port do servidor")
    parser.add_argument("--client-id", type=int, default=1, help="seu ClientID (inteiro)")
    parser.add_argument("--threads", type=int, default=0, help="threads para minerar (0 = cpu_count)")
    parser.add_argument("--pause", action="store_true", help="pausa após operações para ver a saída")
    args = parser.parse_args()

    # Opções de keepalive + backoff para conexões estáveis
    channel = grpc.insecure_channel(
        args.server,
        options=[
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.http2.min_time_between_pings_ms", 30_000),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    )
    stub = pbg.MinerAPIStub(channel)

    try:
        print(f"\n=== Cliente {args.client_id} ===")  # Identifica o cliente

        while True:
            try:
                print_menu()
                op = input(f"Cliente {args.client_id} - Escolha: ").strip()

                if op == "0":
                    print(f"Cliente {args.client_id} - Encerrando.")
                    break

                elif op == "1":
                    txid = stub.getTransactionID(pb.Empty(), timeout=5).id
                    print(f"Cliente {args.client_id} - TransactionID atual: {txid}")

                elif op == "2":
                    txid = int(input("TransactionID: "))
                    # Aqui estamos pegando a dificuldade diretamente antes de minerar
                    challenge = stub.getChallenge(pb.TransactionID(id=txid), timeout=5)
                    diff = challenge.difficulty
                    print(f"Cliente {args.client_id} - Desafio (difficulty): {diff}")

                elif op == "3":
                    txid = int(input("TransactionID: "))
                    st = stub.getTransactionStatus(pb.TransactionID(id=txid), timeout=5).status
                    msg = {0: "RESOLVIDO", 1: "PENDENTE", -1: "INVÁLIDO"}.get(st, f"desconhecido({st})")
                    print(f"Cliente {args.client_id} - Status: {msg}")

                elif op == "4":
                    txid = int(input("TransactionID: "))
                    win = stub.getWinner(pb.TransactionID(id=txid), timeout=5).clientID
                    if win == -1:
                        print("TransactionID inválido.")
                    elif win == 0:
                        print("Ainda sem vencedor.")
                    else:
                        print(f"Cliente {args.client_id} - Vencedor (ClientID): {win}")

                elif op == "5":
                    txid = int(input("TransactionID: "))
                    sol = stub.getSolution(pb.TransactionID(id=txid), timeout=5)
                    msg = {0: "RESOLVIDO", 1: "PENDENTE", -1: "INVÁLIDO"}.get(sol.status, f"desconhecido({sol.status})")
                    print(f"Cliente {args.client_id} - Status: {msg} | Diff: {sol.difficulty} | Solution: {sol.solution}")

                elif op == "6":
                    # 1) pegar tx atual
                    txid = stub.getTransactionID(pb.Empty(), timeout=5).id
                    # 2) pegar challenge (sempre imediatamente antes de minerar)
                    challenge = stub.getChallenge(pb.TransactionID(id=txid), timeout=5)
                    diff = challenge.difficulty
                    print(f"Cliente {args.client_id} - Minerando tx={txid} com dificuldade={diff} ...", flush=True)

                    # 3) minerar localmente
                    th = args.threads if args.threads > 0 else None
                    solution = mine_locally(txid, args.client_id, diff, threads=th)

                    if not solution:
                        print("Falha ao minerar (tempo/threads insuficientes).")
                        continue

                    print(f"Cliente {args.client_id} - Solução local: '{solution}' | sha1={sha1_hex(solution)}", flush=True)

                    # 4) submeter a solução
                    try:
                        rep = stub.submitChallenge(
                            pb.Submission(transactionID=txid, clientID=args.client_id, solution=solution),
                            timeout=5,
                        )
                        code = rep.code
                        meaning = {1: "VÁLIDA/ACEITA", 0: "INVÁLIDA", 2: "JÁ SOLUCIONADO", -1: "ID INVÁLIDO"}.get(code, str(code))
                        print(f"Cliente {args.client_id} - Resposta do servidor: {meaning}", flush=True)
                    except grpc.RpcError as e:
                        print(f"Erro ao submeter: {e.code().name} - {e.details()}")

                else:
                    print("Opção inválida.")
                
                if args.pause:
                    input("Pressione <Enter> para continuar...")

            except ValueError:
                print("Entrada inválida. Tente novamente.")
            except grpc.RpcError as e:
                print(f"Erro RPC: {e.code().name} - {e.details()} (continuando...)")

    except KeyboardInterrupt:
        print(f"\nCliente {args.client_id} - Interrompido pelo usuário. Saindo...")


if __name__ == "__main__":
    main()
