"""Painel web dos protótipos: diagrama dos serviços com o estado de cada um ao vivo,
vazão entre eles e botões para injetar falhas e disparar os scripts de carga e dos cenários.

Roda na máquina, fora do Docker Compose, para sobreviver ao `down -v` que os scripts fazem
entre execuções:

  python -m scripts.painel                 # http://localhost:8090
  python -m scripts.painel --porta 9000

O painel lê o estado dos contêineres com `docker compose ps`, os /healthz e /metrics dos
serviços, o status do conector Debezium e o slot de replicação no banco de origem. As falhas
são os mesmos comandos que o script do Cenário 1 usa (`docker compose stop|kill|pause`).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import aiohttp
import psycopg
from aiohttp import web

from scripts.comum import ambiente as amb

ESTATICO = Path(__file__).parent / "estatico"
PROFILES = ["--profile", "domain-events", "--profile", "cdc"]

# Serviços em que o painel pode injetar falha. Os de inicialização (seed, kafka-init,
# debezium-connector-init) e os marcadores backend-* ficam de fora: reiniciá-los recarregaria
# as bases ou registraria o conector de novo.
ALVOS = {
    "postgres-origem", "postgres-destino", *amb.BROKERS, "kafka-connect",
    *(n for m in amb.MODOS.values() for n in (m["origem"], m["consumidor"])),
}
ACOES = {"stop", "kill", "pause", "unpause", "start", "restart"}
CENARIOS = {"cenario1"}

# Métricas lidas do /metrics, acumuladas como séries de taxa (por segundo).
METRICAS_ORIGEM = ["operations_committed_total", "http_errors_total",
                  "events_published_total", "events_publish_failed_total", "events_retry_pending",
                  "events_late_confirmed_total", "events_retry_recovered_total", "events_declared_lost_total"]
METRICAS_DESTINO = ["messages_consumed_total", "messages_applied_total", "messages_invalid_total",
                     "db_errors_total"]
HISTORICO_S = 600


def _agora() -> str:
    return datetime.now().strftime("%H:%M:%S")


async def _executar(*args: str, timeout: float = 60) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *args, cwd=amb.RAIZ, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        return 124, "", f"tempo esgotado: {' '.join(args)}"
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


async def compose(*args: str, timeout: float = 120) -> tuple[int, str, str]:
    return await _executar("docker", "compose", *PROFILES, *args, timeout=timeout)


def _ler_ps(texto: str) -> list[dict]:
    """`docker compose ps --format json` devolve uma lista (Compose < 2.21) ou um objeto por linha."""
    texto = texto.strip()
    if not texto:
        return []
    if texto.startswith("["):
        return json.loads(texto)
    return [json.loads(l) for l in texto.splitlines() if l.strip().startswith("{")]


class Trabalho:
    """Um script Python disparado pelo painel (carga, Cenário 1 ou troca de backend).
    Só um por vez; a saída fica guardada para a página mostrar."""

    def __init__(self, tipo: str, descricao: str, args: list[str]):
        self.tipo, self.descricao, self.args = tipo, descricao, args
        self.inicio = time.time()
        self.fim: float | None = None
        self.codigo: int | None = None
        self.saida: deque[str] = deque(maxlen=2000)
        self.proc: asyncio.subprocess.Process | None = None

    async def rodar(self, ao_terminar) -> None:
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
        kwargs = {}
        if os.name == "nt":
            kwargs["creationflags"] = 0x00000200  # CREATE_NEW_PROCESS_GROUP, para o CTRL_BREAK
        else:
            kwargs["start_new_session"] = True
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", *self.args, cwd=amb.RAIZ, env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, **kwargs)
        assert self.proc.stdout
        async for linha in self.proc.stdout:
            self.saida.append(linha.decode(errors="replace").rstrip())
        self.codigo = await self.proc.wait()
        self.fim = time.time()
        ao_terminar(self)

    def parar(self) -> None:
        if self.proc is None or self.proc.returncode is not None:
            return
        if os.name == "nt":
            self.proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(self.proc.pid, signal.SIGINT)

    def resumo(self, linhas: int = 400) -> dict:
        return {"tipo": self.tipo, "descricao": self.descricao, "comando": "python -m " + " ".join(self.args),
                "inicio": self.inicio, "fim": self.fim, "codigo": self.codigo,
                "rodando": self.fim is None, "saida": list(self.saida)[-linhas:]}


class Painel:
    def __init__(self, intervalo: float):
        self.intervalo = intervalo
        self.estado: dict = {}
        self.historico: deque[dict] = deque(maxlen=int(HISTORICO_S / intervalo))
        self.eventos: deque[dict] = deque(maxlen=300)
        self.trabalho: Trabalho | None = None
        self._anterior_metricas: dict | None = None
        self._anterior_estados: dict[str, str] = {}
        self._anterior_conector: str | None = None
        self._origem: psycopg.AsyncConnection | None = None

    # ------------------------------------------------------------------ eventos
    def registrar(self, texto: str, tipo: str = "info") -> None:
        self.eventos.append({"t": time.time(), "hora": _agora(), "tipo": tipo, "texto": texto})

    # ------------------------------------------------------------------ amostragem
    async def amostrar_para_sempre(self) -> None:
        async with aiohttp.ClientSession() as sessao:
            while True:
                inicio = time.monotonic()
                try:
                    await self._amostrar(sessao)
                except Exception as e:  # o painel não pode morrer por uma amostra ruim
                    self.registrar(f"erro ao amostrar: {e!r}", "erro")
                await asyncio.sleep(max(0.2, self.intervalo - (time.monotonic() - inicio)))

    async def _amostrar(self, sessao: aiohttp.ClientSession) -> None:
        t = time.time()
        conteineres, origem_h, m_origem, destino_h, m_destino, origem = await asyncio.gather(
            self._conteineres(), self._healthz(sessao, amb.API_URL), amb.ler_metricas(sessao, amb.API_URL),
            self._healthz(sessao, amb.CONSUMIDOR_URL), amb.ler_metricas(sessao, amb.CONSUMIDOR_URL),
            self._origem_slot())
        modo = self._detectar_modo(conteineres, origem_h)
        conector = await self._conector(sessao) if modo == "cdc" else None

        metricas = {k: m_origem.get(k) for k in METRICAS_ORIGEM} | {k: m_destino.get(k) for k in METRICAS_DESTINO}
        taxas = self._taxas(t, metricas)
        self._detectar_mudancas(conteineres, conector)

        self.estado = {"t": t, "modo": modo, "conteineres": conteineres,
                       "healthz": {"origem": origem_h, "destino": destino_h},
                       "metricas": metricas, "taxas": taxas, "conector": conector, "origem": origem}
        fora = sorted(s for s, c in conteineres.items() if s in ALVOS and c["estado"] != "running")
        self.historico.append({"t": t, **taxas, "fora": fora,
                               "slot_atraso_bytes": (origem or {}).get("slot_atraso_bytes")})

    async def _conteineres(self) -> dict[str, dict]:
        rc, out, err = await compose("ps", "--all", "--format", "json", timeout=20)
        if rc != 0:
            return {"_erro": {"mensagem": (err or out).strip()[:500]}}
        res = {}
        for c in _ler_ps(out):
            res[c["Service"]] = {"estado": c.get("State", ""), "saude": c.get("Health", ""),
                                 "status": c.get("Status", ""), "codigo_saida": c.get("ExitCode")}
        return res

    @staticmethod
    async def _healthz(sessao: aiohttp.ClientSession, url: str) -> dict:
        t0 = time.perf_counter()
        try:
            async with sessao.get(f"{url}/healthz", timeout=aiohttp.ClientTimeout(total=2)) as r:
                corpo = await r.text()
                ms = round((time.perf_counter() - t0) * 1000, 1)
                if r.status != 200:
                    return {"ok": False, "ms": ms, "erro": f"HTTP {r.status}: {corpo.strip()[:200]}"}
                return {"ok": True, "ms": ms, "modo": json.loads(corpo).get("mode")}
        except Exception as e:
            return {"ok": False, "erro": type(e).__name__}

    @staticmethod
    def _detectar_modo(conteineres: dict, origem_h: dict) -> str | None:
        for modo, nomes in amb.MODOS.items():
            if conteineres.get(nomes["origem"], {}).get("estado") in ("running", "paused", "restarting"):
                return modo
        for modo, nomes in amb.MODOS.items():
            if nomes["origem"] in conteineres:
                return modo
        return origem_h.get("modo")

    async def _conector(self, sessao: aiohttp.ClientSession) -> dict:
        try:
            async with sessao.get(f"{amb.CONNECT_URL}/connectors/{amb.CONECTOR}/status",
                                  timeout=aiohttp.ClientTimeout(total=2)) as r:
                if r.status == 404:
                    return {"estado": "NAO_REGISTRADO"}
                st = await r.json()
            tarefas = st.get("tasks") or []
            vigia = await amb.ler_metricas(sessao, amb.VIGIA_URL)
            return {"estado": tarefas[0]["state"] if tarefas else st["connector"]["state"],
                    "conector": st["connector"]["state"], "vigia": vigia or None,
                    "erro": (tarefas[0].get("trace") or "").splitlines()[0][:300] if tarefas and tarefas[0].get("trace") else None}
        except Exception:
            return {"estado": "INALCANCAVEL"}

    async def _origem_slot(self) -> dict | None:
        """Disponibilidade do banco de origem e, no CDC, o WAL retido pelo slot do Debezium."""
        try:
            if self._origem is None or self._origem.closed:
                self._origem = await psycopg.AsyncConnection.connect(amb.ORIGEM_DSN, autocommit=True,
                                                                     connect_timeout=2)
            cur = await asyncio.wait_for(self._origem.execute("""
                SELECT s.active,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::bigint,
                       (SELECT coalesce(sum(size), 0) FROM pg_ls_waldir())::bigint
                  FROM (SELECT 1) x LEFT JOIN pg_replication_slots s ON s.slot_name = %s""", (amb.SLOT,)), 3)
            ativo, retido, atraso, wal = await cur.fetchone()
            return {"disponivel": True, "slot_existe": ativo is not None, "slot_ativo": ativo,
                    "slot_retido_bytes": retido, "slot_atraso_bytes": atraso, "wal_bytes": wal}
        except Exception as e:
            if self._origem is not None:
                try:
                    await self._origem.close()
                except Exception:
                    pass
            self._origem = None
            return {"disponivel": False, "erro": type(e).__name__}

    def _taxas(self, t: float, metricas: dict) -> dict:
        anterior, self._anterior_metricas = self._anterior_metricas, {"t": t, **metricas}
        taxas: dict = {}
        for k, v in metricas.items():
            nome = k.removesuffix("_total") + "_s"
            if anterior is None or v is None or anterior.get(k) is None or v < anterior[k]:
                taxas[nome] = None  # serviço fora do ar ou contador zerado por reinício
            else:
                taxas[nome] = round((v - anterior[k]) / max(1e-6, t - anterior["t"]), 1)
        taxas["api_commits_s"] = taxas.get("operations_committed_s")
        return taxas

    def _detectar_mudancas(self, conteineres: dict, conector: dict | None) -> None:
        if "_erro" in conteineres:
            return
        atuais = {s: c["estado"] for s, c in conteineres.items() if s in ALVOS}
        for s, est in atuais.items():
            ant = self._anterior_estados.get(s)
            if ant is not None and ant != est:
                self.registrar(f"{s}: {ant} → {est}", "falha" if est != "running" else "recuperacao")
        for s in set(self._anterior_estados) - set(atuais):
            self.registrar(f"{s}: removido", "info")
        self._anterior_estados = atuais
        estado_conector = conector["estado"] if conector else None
        if estado_conector != self._anterior_conector and self._anterior_conector is not None and estado_conector:
            self.registrar(f"conector Debezium: {self._anterior_conector} → {estado_conector}",
                           "recuperacao" if estado_conector == "RUNNING" else "falha")
        self._anterior_conector = estado_conector

    # ------------------------------------------------------------------ ações
    def _trabalho_bloqueia_falhas(self) -> str | None:
        t = self.trabalho
        if t and t.fim is None and (t.tipo in CENARIOS or t.tipo == "ambiente"):
            return f"aguarde: '{t.descricao}' está em andamento e controla os contêineres"
        return None

    async def injetar(self, servicos: list[str], acao: str) -> dict:
        if acao not in ACOES:
            raise web.HTTPBadRequest(text=f"ação inválida: {acao}")
        invalidos = [s for s in servicos if s not in ALVOS]
        if not servicos or invalidos:
            raise web.HTTPBadRequest(text=f"serviço inválido: {', '.join(invalidos) or '(nenhum)'}")
        if bloqueio := self._trabalho_bloqueia_falhas():
            raise web.HTTPConflict(text=bloqueio)
        comando = [acao, *servicos]
        if acao == "start":
            # docker start direto: o `compose start` religaria também o seed e o kafka-init.
            self.registrar(f"painel: docker start {' '.join(servicos)}", "acao")
            rc, out = await amb.ligar_async(*servicos)
            err = ""
        else:
            self.registrar(f"painel: docker compose {' '.join(comando)}", "acao")
            rc, out, err = await compose(*comando)
        if rc != 0:
            self.registrar(f"falhou: {(err or out).strip()[:300]}", "erro")
        return {"ok": rc == 0, "saida": (out + err).strip()}

    async def restaurar_tudo(self) -> dict:
        """Religa (start/unpause) os serviços do modo no ar que estejam parados ou congelados."""
        if bloqueio := self._trabalho_bloqueia_falhas():
            raise web.HTTPConflict(text=bloqueio)
        cont = self.estado.get("conteineres", {})
        congelados = [s for s, c in cont.items() if s in ALVOS and c["estado"] == "paused"]
        parados = [s for s, c in cont.items() if s in ALVOS and c["estado"] in ("exited", "created", "dead")]
        saidas = []
        if congelados:
            saidas.append(await self.injetar(congelados, "unpause"))
        if parados:
            saidas.append(await self.injetar(parados, "start"))
        if not saidas:
            self.registrar("painel: nada para restaurar", "acao")
        return {"ok": all(s["ok"] for s in saidas), "saida": "\n".join(s["saida"] for s in saidas)}

    async def reiniciar_conector(self) -> dict:
        self.registrar("painel: reiniciando conector e tarefas com falha", "acao")
        async with aiohttp.ClientSession() as sessao:
            ok = await amb.reiniciar_tarefas_falhas(sessao)
        if not ok:
            self.registrar("o Kafka Connect não aceitou o reinício", "erro")
        return {"ok": ok}

    def iniciar_trabalho(self, tipo: str, p: dict) -> dict:
        if self.trabalho and self.trabalho.fim is None:
            raise web.HTTPConflict(text=f"já há um script rodando: {self.trabalho.descricao}")
        args, desc = _montar_comando(tipo, p)
        self.trabalho = Trabalho(tipo, desc, args)
        self.registrar(f"script iniciado: {desc}", "acao")

        def fim(t: Trabalho) -> None:
            self.registrar(f"script terminou ({'ok' if t.codigo == 0 else f'código {t.codigo}'}): {t.descricao}",
                           "acao" if t.codigo == 0 else "erro")

        asyncio.get_running_loop().create_task(self.trabalho.rodar(fim))
        return self.trabalho.resumo()


def _num(p: dict, chave: str, padrao: float, minimo: float, maximo: float) -> str:
    try:
        v = float(p.get(chave, padrao))
    except (TypeError, ValueError):
        raise web.HTTPBadRequest(text=f"{chave} precisa ser um número")
    if not minimo <= v <= maximo:
        raise web.HTTPBadRequest(text=f"{chave} fora do intervalo [{minimo}, {maximo}]")
    return f"{v:g}"


def _montar_comando(tipo: str, p: dict) -> tuple[list[str], str]:
    """Traduz o formulário da página para os argumentos dos scripts existentes. Só valores
    validados entram na linha de comando."""
    if tipo == "ambiente":
        modo = p.get("modo")
        if modo not in amb.MODOS:
            raise web.HTTPBadRequest(text="modo inválido")
        return ["scripts.comum.ambiente", modo], f"subir {modo} (zera as bases)"
    if tipo == "carga":
        perfil = p.get("perfil", "constante")
        if perfil == "constante":
            taxa, dur = _num(p, "taxa", 100, 1, 5000), _num(p, "duracao", 120, 5, 86400)
            return (["scripts.carga.teste_carga", "constante", "--taxa", taxa, "--duracao", dur],
                    f"carga constante {taxa} ops/s por {dur}s")
        if perfil in ("rajada", "represamento"):
            ops, conc = _num(p, "operacoes", 10000, 1, 10_000_000), _num(p, "concorrencia", 200, 1, 5000)
            return (["scripts.carga.teste_carga", perfil, "--operacoes", ops, "--concorrencia", conc],
                    f"carga {perfil} de {ops} operações")
        raise web.HTTPBadRequest(text="perfil de carga inválido")
    if tipo == "cenario1":
        modo = p.get("modo")
        if modo not in ("domain-events", "cdc", "ambos"):
            raise web.HTTPBadRequest(text="modo inválido")
        parada = p.get("parada", "stop")
        if parada not in ("stop", "kill", "pause"):
            raise web.HTTPBadRequest(text="tipo de parada inválido")
        brokers = [b for b in p.get("brokers", amb.BROKERS) if b in amb.BROKERS]
        if not brokers:
            raise web.HTTPBadRequest(text="escolha ao menos um broker")
        janela = _num(p, "janela", 60, 5, 86400)
        args = ["scripts.cenario1.fila_indisponivel", "--modo", modo, "--janela-segundos", janela,
                "--regime", _num(p, "regime", 30, 0, 86400), "--recuperacao", _num(p, "recuperacao", 30, 0, 86400),
                "--taxa", _num(p, "taxa", 100, 1, 5000), "--sem-progresso", _num(p, "sem_progresso", 300, 10, 86400),
                "--parada", parada, "--brokers", ",".join(brokers)]
        if p.get("reiniciar_conector_falho"):
            args.append("--reiniciar-conector-falho")
        if p.get("sem_recuperacao"):
            args.append("--sem-recuperacao")
        return args, f"Cenário 1 ({modo}, janela de {janela}s, {parada} {','.join(brokers)})"
    raise web.HTTPBadRequest(text="tipo de script inválido")


# ---------------------------------------------------------------------- HTTP
def criar_app(painel: Painel) -> web.Application:
    rotas = web.RouteTableDef()

    @rotas.get("/")
    async def index(_):
        return web.FileResponse(ESTATICO / "index.html")

    @rotas.get("/api/estado")
    async def estado(req: web.Request):
        desde = float(req.query.get("eventos_desde", 0))
        t = painel.trabalho
        return web.json_response({
            **painel.estado,
            "historico": list(painel.historico),
            "eventos": [e for e in painel.eventos if e["t"] > desde],
            "trabalho": t.resumo(linhas=int(req.query.get("linhas", 200))) if t else None,
            "alvos": sorted(ALVOS),
        })

    @rotas.post("/api/falha")
    async def falha(req: web.Request):
        corpo = await req.json()
        return web.json_response(await painel.injetar(list(corpo.get("servicos", [])), corpo.get("acao", "")))

    @rotas.post("/api/restaurar")
    async def restaurar(_):
        return web.json_response(await painel.restaurar_tudo())

    @rotas.post("/api/conector/reiniciar")
    async def conector(_):
        return web.json_response(await painel.reiniciar_conector())

    @rotas.post("/api/trabalho")
    async def trabalho(req: web.Request):
        corpo = await req.json()
        return web.json_response(painel.iniciar_trabalho(corpo.get("tipo", ""), corpo.get("parametros", {})))

    @rotas.post("/api/trabalho/parar")
    async def parar(_):
        if painel.trabalho:
            painel.trabalho.parar()
            painel.registrar(f"painel: interrompendo {painel.trabalho.descricao}", "acao")
        return web.json_response({"ok": True})

    # Exigir JSON nas ações impede que outra página aberta no navegador dispare uma falha com um
    # POST simples de formulário: um POST cross-origin com JSON passa por preflight e é barrado.
    @web.middleware
    async def so_json(req: web.Request, handler):
        if req.method == "POST" and req.content_type != "application/json":
            raise web.HTTPUnsupportedMediaType(text="use Content-Type: application/json")
        return await handler(req)

    app = web.Application(middlewares=[so_json])
    app.add_routes(rotas)
    app.router.add_static("/estatico", ESTATICO)

    async def iniciar(app):
        app["amostrador"] = asyncio.create_task(painel.amostrar_para_sempre())

    async def encerrar(app):
        app["amostrador"].cancel()
        if painel.trabalho:
            painel.trabalho.parar()

    app.on_startup.append(iniciar)
    app.on_cleanup.append(encerrar)
    return app


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--porta", type=int, default=8090)
    p.add_argument("--host", default="127.0.0.1",
                   help="interface de escuta (padrão: só a máquina local, porque o painel controla o Docker)")
    p.add_argument("--intervalo", type=float, default=1.0, help="segundos entre amostras")
    a = p.parse_args()
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    painel = Painel(a.intervalo)
    painel.registrar("painel iniciado", "info")
    print(f"Painel em http://localhost:{a.porta}  (Ctrl+C para sair)")
    web.run_app(criar_app(painel), host=a.host, port=a.porta, print=None)
