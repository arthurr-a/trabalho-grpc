import grpc
from concurrent import futures
import grpcCalc_pb2
import grpcCalc_pb2_grpc

class CalculatorServicer(grpcCalc_pb2_grpc.apiServicer):
    def add(self, request, context):
        return grpcCalc_pb2.result(num=(request.numOne + request.numTwo))

    def sub(self, request, context):
        return grpcCalc_pb2.result(num=(request.numOne - request.numTwo))

    def mul(self, request, context):
        return grpcCalc_pb2.result(num=(request.numOne * request.numTwo))

    def div(self, request, context):
        if request.numTwo == 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Divisão por zero")
            return grpcCalc_pb2.result()
        return grpcCalc_pb2.result(num=(request.numOne / request.numTwo))

def serve():
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    grpcCalc_pb2_grpc.add_apiServicer_to_server(CalculatorServicer(), grpc_server)
    grpc_server.add_insecure_port('[::]:8080')
    grpc_server.start()
    grpc_server.wait_for_termination()

if __name__ == '__main__':
    serve()
