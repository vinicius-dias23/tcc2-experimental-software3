"""Controle do ambiente Docker Compose e leitura de sinais dos serviços.

Também serve para alternar o backend no ar, derrubando o outro e zerando as bases:
  python -m scripts.comum.ambiente domain-events
  python -m scripts.comum.ambiente cdc
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path

import aiohttp

RAIZ = Path(__file__).resolve().parents[2]

API_URL = os.environ.get("API_URL", "http://localhost:8080")
CONSUMIDOR_URL = os.environ.get("CONSUMIDOR_URL", "http://localhost:8081")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
VIGIA_URL = os.environ.get("VIGIA_URL", "http://localhost:8086")
ORIGEM_DSN = os.environ.get("ORIGEM_DSN", "postgresql://retailrocket:retailrocket@localhost:5432/retailrocket")
DESTINO_DSN = os.environ.get("DESTINO_DSN", "postgresql://retailrocket:retailrocket@localhost:5433/retailrocket")
CONECTOR = os.environ.get("CONNECTOR_NAME", "retailrocket-origem")
SLOT = os.environ.get("SLOT_NAME", "retailrocket_debezium")

# Cenário 2: réplica física da origem, publicada no host na porta 5434.
PRIMARIO = "postgres-origem"
REPLICA = "postgres-origem-replica"
REPLICA_DSN = os.environ.get("REPLICA_DSN", "postgresql://retailrocket:retailrocket@localhost:5434/retailrocket")

MODOS = {
    "domain-events": {"alvo": "backend-domain-events", "profile": "domain-events",
                      "origem": "tracking-service-domain-events", "consumidor": "analytics-service-domain-events"},
    "cdc": {"alvo": "backend-change-data-capture", "profile": "cdc",
            "origem": "tracking-service-cdc", "consumidor": "analytics-service-cdc"},
}
BROKERS = ["kafka-1", "kafka-2", "kafka-3"]

# Cenário 5: instâncias consumidoras do mesmo grupo. A 1 é a de sempre; a 2 e a 3 só sobem
# quando pedidas. Cada uma publica /healthz, /metrics e /group numa porta do host.
PORTAS_CONSUMIDORES = {1: 8081, 2: 8084, 3: 8085}


def consumidor(modo: str, instancia: int) -> str:
    base = MODOS[modo]["consumidor"]
    return base if instancia == 1 else f"{base}-{instancia}"


def url_consumidor(instancia: int) -> str:
    if instancia == 1:
        return CONSUMIDOR_URL
    return os.environ.get(f"CONSUMIDOR_{instancia}_URL", f"http://localhost:{PORTAS_CONSUMIDORES[instancia]}")


def compose(*args: str, check: bool = True, capturar: bool = False, timeout: float | None = None
            ) -> subprocess.CompletedProcess:
    """Executa `docker compose` na raiz do repositório com os dois profiles habilitados."""
    cmd = ["docker", "compose", "--profile", "domain-events", "--profile", "cdc", *args]
    r = subprocess.run(cmd, cwd=RAIZ, text=True, timeout=timeout, capture_output=True)
    if not capturar:
        print(r.stdout + r.stderr, end="", flush=True)
    if check and r.returncode != 0:
        raise SystemExit(f"falhou: {' '.join(cmd)}\n{r.stdout}{r.stderr}")
    return r


async def compose_async(*args: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        "docker", "compose", "--profile", "domain-events", "--profile", "cdc", *args,
        cwd=RAIZ, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return proc.returncode, out.decode(errors="replace")


async def ligar_async(*servicos: str) -> tuple[int, str]:
    """Religa contêineres parados com `docker start`, sem passar pelo Compose.

    `docker compose start` também religa as dependências de inicialização (seed e kafka-init),
    o que acrescentaria uns 15 s de indisponibilidade a cada religamento do tracking-service,
    do consumidor ou do Kafka Connect."""
    ids = []
    for s in servicos:
        rc, out = await compose_async("ps", "-aq", s)
        if rc != 0 or not out.strip():
            return 1, f"contêiner de {s} não encontrado"
        ids += out.split()
    proc = await asyncio.create_subprocess_exec("docker", "start", *ids, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return proc.returncode, out.decode(errors="replace")


# Mecanismos de recuperação automática (M1 no Software A, M4 no Software B), com a mesma
# política nos dois: tentativas, espera antes da primeira e multiplicador das seguintes.
RECUPERACAO_PADRAO = {
    "PUBLISH_RETRY_ATTEMPTS": "3", "PUBLISH_RETRY_BACKOFF": "1s", "PUBLISH_RETRY_MULTIPLIER": "2",
    "PUBLISH_RETRY_BUFFER": "10000",
    "CONNECT_RESTART_ATTEMPTS": "3", "CONNECT_RESTART_BACKOFF": "1s", "CONNECT_RESTART_MULTIPLIER": "2",
    "CONNECT_RESTART_RESET_AFTER": "60s",
}


def definir_recuperacao(ligada: bool) -> None:
    """Liga ou desliga a recuperação automática nos próximos `up` (as variáveis do ambiente do
    processo valem mais que o .env para o Compose). Desligada, os dois softwares voltam ao
    comportamento sem retentativa: o A descarta o evento e o B deixa a tarefa em FAILED."""
    if not ligada:
        os.environ["PUBLISH_RETRY_ATTEMPTS"] = "0"
        os.environ["CONNECT_RESTART_ATTEMPTS"] = "0"


def parametros_recuperacao() -> dict[str, str]:
    """Valores efetivos da recuperação automática, para registrar nos marcos de cada execução:
    variável do processo, senão o .env da raiz, senão o padrão do docker-compose.yml."""
    arquivo = {}
    env = RAIZ / ".env"
    if env.exists():
        for linha in env.read_text().splitlines():
            chave, sep, valor = linha.partition("=")
            if sep and not linha.lstrip().startswith("#"):
                arquivo[chave.strip()] = valor.strip()
    return {k: os.environ.get(k) or arquivo.get(k) or v for k, v in RECUPERACAO_PADRAO.items()}


def reiniciar_ambiente(modo: str, build: bool = False) -> None:
    """Derruba tudo (inclusive volumes) e sobe o backend do modo pedido, esperando ficar saudável.

    Reproduz a reinicialização integral entre execuções descrita na Seção 4.3: bancos
    recarregados do seed, tópicos recriados e slot de replicação recriado.
    """
    alvo = MODOS[modo]["alvo"]
    print("[ambiente] derrubando o ambiente anterior")
    compose("down", "-v", "--remove-orphans")
    print(f"[ambiente] subindo {alvo}")
    args = ["up", "-d", "--wait", "--wait-timeout", "1800"]
    if build:
        args.append("--build")
    compose(*args, alvo)
    esperar_http(f"{API_URL}/healthz", 120)
    esperar_http(f"{CONSUMIDOR_URL}/healthz", 120)
    if modo == "cdc":
        esperar_conector_running(180)
    print(f"[ambiente] {alvo} pronto")


def esperar_http(url: str, timeout_s: float) -> None:
    import urllib.request
    limite = time.time() + timeout_s
    while True:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        if time.time() > limite:
            raise SystemExit(f"{url} não respondeu em {timeout_s:.0f}s")
        time.sleep(1)


def esperar_conector_running(timeout_s: float) -> None:
    import urllib.request
    limite = time.time() + timeout_s
    while time.time() < limite:
        try:
            with urllib.request.urlopen(f"{CONNECT_URL}/connectors/{CONECTOR}/status", timeout=3) as r:
                st = json.load(r)
                if st["connector"]["state"] == "RUNNING" and st["tasks"] and \
                        all(t["state"] == "RUNNING" for t in st["tasks"]):
                    return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit("o conector Debezium não chegou a RUNNING")


async def failover_origem(atraso_promocao: float = 0.0, log=print, apos_queda=None, apos_promocao=None) -> dict:
    """Simula o failover do banco de origem (Cenário 2): encerra o primário sem aviso (SIGKILL),
    promove a réplica e passa o nome `postgres-origem` para ela, como o endpoint de um banco
    gerenciado faz ao trocar de nó. Os serviços reconectam ao mesmo nome e chegam à réplica.

    Devolve os instantes de cada passo. Levanta RuntimeError se algum passo falhar."""
    marcos: dict = {}
    rc, out = await compose_async("kill", "-s", "SIGKILL", PRIMARIO)
    marcos["primario_derrubado"] = time.time()
    if rc != 0:
        raise RuntimeError(f"não foi possível derrubar o primário: {out}")
    log(f"primário encerrado (SIGKILL); promovendo a réplica em {atraso_promocao:g}s")
    if apos_queda is not None:
        await apos_queda()
    await asyncio.sleep(atraso_promocao)
    out = await psql_replica("SELECT pg_promote(true, 60)")
    marcos["promocao_concluida"] = time.time()
    if "t" not in out.split():
        raise RuntimeError(f"a promoção da réplica falhou: {out}")
    log("réplica promovida a primário")
    if apos_promocao is not None:
        await apos_promocao()  # ainda sem tráfego: nada além da réplica alcança o novo primário
    await apontar_origem_para_replica()
    marcos["endereco_trocado"] = time.time()
    log("o nome postgres-origem agora aponta para a réplica promovida")
    return marcos


async def _rede_da_replica() -> tuple[str, str]:
    """(id do contêiner da réplica, rede do Compose)."""
    rc, cid = await compose_async("ps", "-q", REPLICA)
    cid = cid.strip()
    if rc != 0 or not cid:
        raise RuntimeError("réplica não encontrada")
    rc, nome = await compose_async("config", "--format", "json")
    try:
        projeto = json.loads(nome)["name"]
    except Exception:
        projeto = "retailrocket-tcc"
    return cid, f"{projeto}_default"


async def _docker(*args: str, falha_ok: bool = False) -> str:
    proc = await asyncio.create_subprocess_exec("docker", *args, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    if proc.returncode != 0 and not falha_ok:
        raise RuntimeError(f"docker {' '.join(args)}: {out.decode(errors='replace')}")
    return out.decode(errors="replace")


async def isolar_replica() -> None:
    """Tira a réplica da rede (partição de rede entre primário e réplica): o que o primário
    commitar a partir daqui não chega a ela. Congelar o contêiner não serve, porque o kernel
    continua aceitando o WAL no buffer do socket e a réplica o lê ao descongelar."""
    cid, rede = await _rede_da_replica()
    await _docker("network", "disconnect", rede, cid)


async def apontar_origem_para_replica() -> None:
    """Liga a réplica à rede do Compose com o apelido `postgres-origem` (além do próprio nome).
    O primário antigo precisa estar parado, senão o nome resolveria para os dois."""
    cid, rede = await _rede_da_replica()
    await _docker("network", "disconnect", rede, cid, falha_ok=True)
    await _docker("network", "connect", "--alias", PRIMARIO, "--alias", REPLICA, rede, cid)


async def psql_replica(sql: str) -> str:
    """Executa SQL na réplica por `docker compose exec`, o que funciona mesmo com ela fora da rede."""
    rc, out = await compose_async("exec", "-T", REPLICA, "psql", "-U", "retailrocket", "-d", "retailrocket",
                                  "-AtX", "-c", sql)
    if rc != 0:
        raise RuntimeError(f"psql na réplica falhou: {out}")
    return out


async def ler_metricas(sessao: aiohttp.ClientSession, url: str) -> dict[str, float]:
    """Lê o /metrics (texto Prometheus) de um serviço. Devolve {} se não responder."""
    try:
        async with sessao.get(f"{url}/metrics", timeout=aiohttp.ClientTimeout(total=3)) as r:
            texto = await r.text()
    except Exception:
        return {}
    out = {}
    for linha in texto.splitlines():
        if linha and not linha.startswith("#"):
            nome, _, valor = linha.partition(" ")
            try:
                out[nome] = float(valor)
            except ValueError:
                pass
    return out


async def estado_conector(sessao: aiohttp.ClientSession) -> str:
    """Estado da tarefa Debezium (RUNNING, FAILED, PAUSED...) ou INALCANCAVEL."""
    try:
        async with sessao.get(f"{CONNECT_URL}/connectors/{CONECTOR}/status",
                              timeout=aiohttp.ClientTimeout(total=3)) as r:
            st = await r.json()
        tarefas = st.get("tasks") or []
        return tarefas[0]["state"] if tarefas else st["connector"]["state"]
    except Exception:
        return "INALCANCAVEL"


async def reiniciar_tarefas_falhas(sessao: aiohttp.ClientSession) -> bool:
    """Pede ao Connect que reinicie conector e tarefas com falha (intervenção operacional)."""
    try:
        async with sessao.post(f"{CONNECT_URL}/connectors/{CONECTOR}/restart?includeTasks=true&onlyFailed=true",
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            return r.status < 300
    except Exception:
        return False


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2 or sys.argv[1] not in MODOS:
        raise SystemExit(f"uso: python -m scripts.comum.ambiente {{{'|'.join(MODOS)}}}")
    reiniciar_ambiente(sys.argv[1])
