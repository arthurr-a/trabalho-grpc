import grpc
import grpcCalc_pb2
import grpcCalc_pb2_grpc
import pybreaker

breaker = pybreaker.CircuitBreaker(fail_max=2, reset_timeout=2)

@breaker
def run_client():
    channel = grpc.insecure_channel('localhost:8080')
    client = grpcCalc_pb2_grpc.apiStub(channel)

    while True:
        print("\n=== Calculadora RPC ===")
        print("1 - Soma")
        print("2 - Subtração")
        print("3 - Multiplicação")
        print("4 - Divisão")
        print("0 - Sair")
        op = input("Escolha a operação: ").strip()

        if op == '0':
            print("Encerrando cliente.")
            break

        try:
            x = float(input('Entre com o primeiro número: '))
            y = float(input('Entre com o segundo número: '))
        except ValueError:
            print("Entrada inválida. Tente novamente.")
            continue

        try:
            if op == '1':
                res = client.add(grpcCalc_pb2.args(numOne=x, numTwo=y))
            elif op == '2':
                res = client.sub(grpcCalc_pb2.args(numOne=x, numTwo=y))
            elif op == '3':
                res = client.mul(grpcCalc_pb2.args(numOne=x, numTwo=y))
            elif op == '4':
                res = client.div(grpcCalc_pb2.args(numOne=x, numTwo=y))
            else:
                print("Opção inválida.")
                continue

            print(f"Resultado: {res.num}")
        except grpc.RpcError as e:
            print(f"Erro RPC: {e.code().name} - {e.details()}")
        except pybreaker.CircuitBreakerError:
            print("Circuit breaker aberto, tentando novamente mais tarde.")

if __name__ == '__main__':
    run_client()
