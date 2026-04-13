# rmq-mover

Ferramenta CLI para mover ou copiar mensagens entre filas RabbitMQ, com sintaxe inspirada no `scp`.

## Instalação

Requer [Go 1.21+](https://go.dev/dl/).

```sh
go mod tidy
go build -o rmq-mover .        # Linux/macOS
go build -o rmq-mover.exe .    # Windows
```

## Uso

```
rmq-mover [opções] SOURCE DEST
```

### Formato de SOURCE e DEST

```
[user:pass@]host[:port][/vhost]/fila
```

| Parte | Padrão | Descrição |
|---|---|---|
| `user:pass@` | `guest:guest` | Credenciais (opcional) |
| `host` | — | Hostname ou IP do servidor |
| `:port` | `5672` | Porta AMQP (opcional) |
| `/vhost` | `/` | Virtual host (opcional) |
| `/fila` | — | Nome da fila (obrigatório) |

### Opções

| Flag | Atalho | Padrão | Descrição |
|---|---|---|---|
| `--copy` | `-c` | `false` | Copiar mensagens (mantém na origem) |
| `--count N` | `-n N` | `0` | Máximo de mensagens (0 = todas) |
| `--workers N` | `-w N` | `4` | Workers paralelos |
| `--timeout N` | `-t N` | `5` | Segundos sem mensagens novas para encerrar |

## Exemplos

```sh
# Mover todas as mensagens entre filas no mesmo servidor
rmq-mover guest:pass@localhost/fila-origem guest:pass@localhost/fila-destino

# Mover entre servidores diferentes
rmq-mover guest:pass@rabbit1.exemplo.com/fila1 admin:senha@rabbit2.exemplo.com/fila2

# Copiar mensagens (mantém na origem)
rmq-mover --copy guest:pass@localhost/fila1 guest:pass@localhost/fila2

# Mover com vhost explícito
rmq-mover guest:pass@localhost/meu-vhost/fila1 guest:pass@localhost/meu-vhost/fila2

# Mover apenas as primeiras 1000 mensagens
rmq-mover -n 1000 guest:pass@localhost/fila1 guest:pass@localhost/fila2

# Mover com porta customizada e 8 workers paralelos
rmq-mover -w 8 guest:pass@rabbit1:5673/fila1 guest:pass@rabbit2:5673/fila2

# Aguardar até 30s sem mensagens antes de encerrar (útil para filas com consumo lento)
rmq-mover -t 30 guest:pass@localhost/fila1 guest:pass@localhost/fila2
```

## Comportamento

### Modo move (padrão)

Cada mensagem segue o fluxo:

```
1. Publica na fila destino
2. Aguarda confirmação do broker destino (publisher confirms)
3. Remove da fila origem (ack)
```

Nenhuma mensagem é perdida: se a publicação falhar, a mensagem permanece na origem.

### Modo copy (`--copy`)

```
1. Publica na fila destino
2. Aguarda confirmação do broker destino
3. Republica na fila origem (preserva metadados)
4. Remove o delivery original (ack)
```

> **Nota:** o modo copy não preserva a ordem das mensagens na fila origem.

### Uso de memória

As mensagens são processadas em lotes via AMQP prefetch (`workers × 4`). A fila nunca é carregada inteiramente em memória — com 4 workers, no máximo 64 mensagens ficam em buffer simultaneamente.

### Encerramento automático

O processo encerra quando:
- Todas as mensagens disponíveis foram processadas
- O limite `--count` foi atingido
- Nenhuma mensagem nova chega dentro do `--timeout` (padrão: 5s)

## Progresso

Durante a execução é exibida uma barra de progresso em tempo real:

```
Moving 5000 messages
  from: localhost:5672/fila-origem
    to: localhost:5672/fila-destino

[===============>               ] 2500/5000 | 834 msg/s | 3.0s
Done. 5000 messages moved.
```
