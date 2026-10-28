"use strict";

// ------------------------------------------------------------------ utilidades
const $ = (s) => document.querySelector(s);
const NS = "http://www.w3.org/2000/svg";
function el(tag, attrs = {}, pai) {
  const e = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (pai) pai.appendChild(e);
  return e;
}
function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
const fmt = (v, casas = 0) => (v == null ? "–" : Number(v).toLocaleString("pt-BR", { maximumFractionDigits: casas }));
function bytes(b) {
  if (b == null) return "–";
  const u = ["B", "KB", "MB", "GB"];
  let i = 0;
  while (Math.abs(b) >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return `${b.toFixed(i >= 2 ? 1 : 0)} ${u[i]}`;
}
async function api(caminho, corpo) {
  const r = await fetch(caminho, corpo === undefined ? {} : {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(corpo),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

// ------------------------------------------------------------------ estado da página
let E = null;               // último /api/estado
let selecionado = null;     // chave do nó selecionado
let ultimoEvento = 0;
const eventos = [];
const BROKERS = ["kafka-1", "kafka-2", "kafka-3"];
const ROTULO_MODO = { "domain-events": "Software A · Domain Events", cdc: "Software B · CDC" };

// Nós do diagrama: posição, rótulos e como achar o contêiner no modo atual.
const NOS = {
  cliente: { x: 20,  y: 60,  titulo: "Gerador de carga", papel: "scripts/carga · cenário" },
  origem:  { x: 240, y: 60,  titulo: "tracking-service", papel: "origem · API :8080", servico: (m) => `tracking-service-${m}` },
  pgo:     { x: 240, y: 300, titulo: "postgres-origem", papel: ":5432 · wal_level=logical", servico: () => "postgres-origem" },
  pgr:     { x: 20,  y: 300, titulo: "réplica da origem", papel: ":5434 · Cenário 2", servico: () => "postgres-origem-replica", opcional: true },
  connect: { x: 505, y: 300, titulo: "Debezium / Connect", papel: "lê o WAL · :8083", servico: () => "kafka-connect", soCdc: true },
  pgd:     { x: 780, y: 300, titulo: "postgres-destino", papel: ":5433", servico: () => "postgres-destino" },
};
BROKERS.forEach((b, i) => { NOS[b] = { x: 490, y: 52 + i * 46, w: 200, h: 38, titulo: b, broker: true, servico: () => b }; });
// Instâncias do analytics-service no mesmo grupo de consumo. A 2 e a 3 só existem no Cenário 5.
const PORTAS_CONSUMIDOR = { 1: 8081, 2: 8084, 3: 8085 };
const CONSUMIDORES = [1, 2, 3].map((i) => `dest${i}`);
[1, 2, 3].forEach((i) => {
  NOS[`dest${i}`] = { x: 780, y: 52 + (i - 1) * 46, w: 200, h: 38, titulo: `instância ${i}`, porta: PORTAS_CONSUMIDOR[i], broker: true,
    consumidor: i, opcional: i > 1, servico: (m) => `analytics-service-${m}${i > 1 ? `-${i}` : ""}` };
});
const W = 170, H = 78;

// Arestas: de onde para onde, e qual métrica vai no rótulo.
const ARESTAS = [
  { id: "cli-origem", d: "M190 99 L236 99", rotulo: [213, 86], de: "cliente", para: "origem" },
  { id: "origem-pgo", d: "M325 138 L325 296", rotulo: [332, 222], ancora: "start", de: "origem", para: "pgo" },
  { id: "origem-kafka", d: "M410 99 L476 99", rotulo: [443, 86], de: "origem", para: "kafka", soModo: "domain-events" },
  { id: "pgo-connect", d: "M410 339 L501 339", rotulo: [455, 326], de: "pgo", para: "connect", soModo: "cdc" },
  { id: "connect-kafka", d: "M590 300 L590 194", rotulo: [597, 252], ancora: "start", de: "connect", para: "kafka", soModo: "cdc" },
  { id: "kafka-destino", d: "M700 99 L766 99", rotulo: [733, 86], de: "kafka", para: "consumidores" },
  { id: "destino-pgd", d: "M880 198 L880 296", rotulo: [887, 252], ancora: "start", de: "consumidores", para: "pgd" },
  { id: "pgo-pgr", d: "M236 339 L194 339", rotulo: [215, 326], de: "pgo", para: "pgr", tracejada: true },
];

// ------------------------------------------------------------------ classificação de estado
// Devolve {cls, texto} a partir do contêiner (docker compose ps) e, se houver, do /healthz.
function classificar(servico, healthz) {
  const c = E?.conteineres?.[servico];
  if (!c) return { cls: "ausente", texto: "não criado" };
  if (c.estado === "paused") return { cls: "congelado", texto: "congelado" };
  if (c.estado === "restarting") return { cls: "aviso", texto: "reiniciando" };
  if (c.estado !== "running") {
    return { cls: "erro", texto: "parado" };
  }
  if (c.saude === "starting") return { cls: "aviso", texto: "iniciando" };
  if (c.saude === "unhealthy") return { cls: "aviso", texto: "não saudável" };
  if (healthz && !healthz.ok) return { cls: "aviso", texto: "sem resposta" };
  return { cls: "ok", texto: "no ar" };
}

function estadoNo(chave) {
  const modo = E?.modo;
  const no = NOS[chave];
  if (chave === "cliente") {
    const t = E?.trabalho;
    if (t?.rodando) return { cls: "ok", texto: t.tipo === "ambiente" ? "subindo backend" : "rodando" };
    return { cls: "ausente", texto: "ocioso" };
  }
  if (no.soCdc && modo !== "cdc") return { cls: "ausente", texto: "só no Software B", inativo: true };
  if (!modo && !(no.broker && !no.consumidor) && !["pgo", "pgd", "pgr"].includes(chave)) return { cls: "ausente", texto: "nenhum backend" };
  const servico = no.servico(modo || "domain-events");
  if (no.opcional && !E?.conteineres?.[servico]) {
    return { cls: "ausente", texto: chave === "pgr" ? "só no Cenário 2" : "Cenário 5", inativo: true };
  }
  const hz = chave === "origem" ? E?.healthz?.origem : no.consumidor ? E?.consumidores?.[no.consumidor]?.healthz : null;
  const st = classificar(servico, hz);
  if (chave === "pgr" && st.cls === "ok" && E?.replica) {
    if (!E.replica.disponivel) return { cls: "aviso", texto: "fora da rede" };
    if (E.replica.papel === "primario") return { cls: "ok", texto: "primário (promovida)" };
    const atraso = E?.origem?.replica_atraso_bytes;
    return { cls: "ok", texto: atraso == null ? "standby" : `standby · ${bytes(atraso)}` };
  }
  if (chave === "connect" && st.cls === "ok" && E?.conector) {
    const ce = E.conector.estado;
    if (ce === "FAILED") return { cls: "erro", texto: "tarefa FAILED" };
    if (ce !== "RUNNING") return { cls: "aviso", texto: ce.toLowerCase().replace("_", " ") };
  }
  if (chave === "pgo" && st.cls === "ok" && E?.origem && !E.origem.disponivel) return { cls: "aviso", texto: "recusando conexões" };
  return st;
}

function estadoConsumidores() {
  const g = E?.grupo;
  const vivos = CONSUMIDORES.filter((k) => estadoNo(k).cls === "ok").length;
  if (!E?.modo) return { cls: "ausente", texto: "nenhum backend" };
  if (g?.estado) {
    const nome = { Stable: "estável", PreparingRebalance: "rebalanceando", CompletingRebalance: "rebalanceando", Empty: "vazio", Dead: "removido" }[g.estado] || g.estado;
    const texto = `${nome} · ${g.membros}`;
    if (g.estado === "Stable") return { cls: vivos ? "ok" : "aviso", texto };
    if (g.estado === "Empty" || g.estado === "Dead") return { cls: "erro", texto };
    return { cls: "aviso", texto };
  }
  return vivos ? { cls: "aviso", texto: "grupo sem resposta" } : { cls: "erro", texto: "nenhuma instância" };
}

function estadoFila() {
  const no = BROKERS.filter((b) => estadoNo(b).cls === "ok").length;
  if (no === 3) return { cls: "ok", texto: "3/3 brokers" };
  if (no >= 2) return { cls: "aviso", texto: `${no}/3 brokers` };
  return { cls: "erro", texto: `${no}/3 · sem quórum` };
}

// ------------------------------------------------------------------ diagrama
const svg = $("#diagrama");
const refs = {};

function montarDiagrama() {
  const defs = el("defs", {}, svg);
  const m = el("marker", { id: "ponta", viewBox: "0 0 10 10", refX: 8, refY: 5, markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse" }, defs);
  el("path", { d: "M0 0 L10 5 L0 10 z", class: "seta" }, m);

  const g = el("g", { class: "grupo", tabindex: 0 }, svg);
  el("rect", { x: 480, y: 20, width: 220, height: 174, rx: 10 }, g);
  const tg = el("text", { x: 490, y: 40 }, g);
  tg.textContent = "Kafka";
  refs.grupo = { g, estado: el("text", { x: 690, y: 40, "text-anchor": "end" }, g) };
  g.addEventListener("click", (ev) => { if (ev.target === g.firstChild || ev.target === tg || ev.target === refs.grupo.estado) selecionar("kafka"); });

  const gc = el("g", { class: "grupo", tabindex: 0 }, svg);
  el("rect", { x: 770, y: 20, width: 220, height: 174, rx: 10 }, gc);
  const tc = el("text", { x: 780, y: 40 }, gc);
  tc.textContent = "analytics-service";
  refs.consumidores = { g: gc, estado: el("text", { x: 980, y: 40, "text-anchor": "end" }, gc) };
  gc.addEventListener("click", (ev) => { if (ev.target === gc.firstChild || ev.target === tc || ev.target === refs.consumidores.estado) selecionar("consumidores"); });

  for (const a of ARESTAS) {
    const ga = el("g", { class: "aresta" }, svg);
    el("path", { d: a.d, "marker-end": "url(#ponta)", ...(a.tracejada ? { "stroke-dasharray": "5 4" } : {}) }, ga);
    const t = el("text", { x: a.rotulo[0], y: a.rotulo[1], "text-anchor": a.ancora || "middle" }, ga);
    const vertical = !!a.ancora;
    const t2 = el("text", { class: "alerta", x: a.rotulo[0], y: a.rotulo[1] + (vertical ? 15 : 30), "text-anchor": a.ancora || "middle" }, ga);
    refs[a.id] = { g: ga, t, t2 };
  }

  for (const [chave, no] of Object.entries(NOS)) {
    const w = no.w || W, h = no.h || H;
    const gn = el("g", { class: "no", tabindex: 0, role: "button", "aria-label": no.titulo }, svg);
    el("rect", { class: "caixa", x: no.x, y: no.y, width: w, height: h, rx: 8 }, gn);
    const faixa = el("rect", { class: "faixa", x: no.x, y: no.y + 6, width: 5, height: h - 12, rx: 2.5 }, gn);
    const bol = el("circle", { cx: no.broker ? no.x + w - 14 : no.x + 20, cy: no.y + (no.broker ? h / 2 : 58), r: 5.5 }, gn);
    const tt = el("text", { class: "titulo", x: no.x + 14, y: no.y + (no.broker ? h / 2 + 5 : 22) }, gn);
    tt.textContent = no.titulo;
    let est;
    if (no.broker) {
      est = el("text", { class: "estado", x: no.x + w - 26, y: no.y + h / 2 + 4, "text-anchor": "end" }, gn);
    } else {
      const pp = el("text", { class: "papel", x: no.x + 14, y: no.y + 40 }, gn);
      pp.textContent = no.papel;
      est = el("text", { class: "estado", x: no.x + 32, y: no.y + 62 }, gn);
    }
    gn.addEventListener("click", (ev) => { ev.stopPropagation(); selecionar(chave); });
    gn.addEventListener("keydown", (ev) => { if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); selecionar(chave); } });
    refs[chave] = { g: gn, faixa, bol, est };
  }
}

function pintar(ref, st) {
  for (const alvo of [ref.faixa, ref.bol]) alvo.setAttribute("class", alvo === ref.faixa ? `faixa ${st.cls}-f` : `${st.cls}-f`);
  ref.est.textContent = st.texto;
}

function taxa(nome) { return E?.taxas?.[nome]; }

function atualizarDiagrama() {
  const modo = E?.modo;
  const st = {};
  for (const chave of Object.keys(NOS)) {
    st[chave] = estadoNo(chave);
    pintar(refs[chave], st[chave]);
    refs[chave].g.classList.toggle("inativo", !!st[chave].inativo);
    refs[chave].g.classList.toggle("selecionado", selecionado === chave);
  }
  st.kafka = estadoFila();
  refs.grupo.estado.textContent = st.kafka.texto;
  refs.grupo.estado.setAttribute("class", `${st.kafka.cls}-f`);
  refs.grupo.g.classList.toggle("selecionado", selecionado === "kafka");
  st.consumidores = estadoConsumidores();
  refs.consumidores.estado.textContent = st.consumidores.texto;
  refs.consumidores.estado.setAttribute("class", `${st.consumidores.cls}-f`);
  refs.consumidores.g.classList.toggle("selecionado", selecionado === "consumidores");

  const fora = (k) => ["erro", "congelado"].includes(st[k].cls);
  const rot = {
    "cli-origem": () => [`${fmt(taxa("api_commits_s"))}/s`, (taxa("http_errors_s") || 0) > 0 ? `${fmt(taxa("http_errors_s"))} erros/s` : null],
    "origem-pgo": () => ["eventos · visitantes · itens", null],
    "origem-kafka": () => [`${fmt(taxa("events_published_s"))}/s`, (taxa("events_publish_failed_s") || 0) > 0 ? `${fmt(taxa("events_publish_failed_s"))} falhas` : null],
    "pgo-connect": () => {
      const o = E?.origem;
      return ["WAL", o?.slot_existe && o.slot_ativo === false ? "slot inativo" : null, o?.slot_existe ? `atraso ${bytes(o.slot_atraso_bytes)}` : null];
    },
    "connect-kafka": () => [E?.conector?.estado ? E.conector.estado.toLowerCase() : "–", null],
    "kafka-destino": () => [`${fmt(taxa("messages_consumed_s"))}/s`, null],
    "destino-pgd": () => [`${fmt(taxa("messages_applied_s"))} aplicadas/s`, (taxa("db_errors_s") || 0) > 0 ? `${fmt(taxa("db_errors_s"))} erros/s` : null],
    "pgo-pgr": () => ["", null],
  };
  const fluxo = {
    "cli-origem": taxa("api_commits_s"), "origem-pgo": taxa("api_commits_s"), "origem-kafka": taxa("events_published_s"),
    "pgo-connect": E?.origem?.slot_ativo ? 1 : 0, "connect-kafka": E?.conector?.estado === "RUNNING" && fora("kafka") === false ? taxa("messages_consumed_s") : 0,
    "kafka-destino": taxa("messages_consumed_s"), "destino-pgd": taxa("messages_applied_s"),
    "pgo-pgr": E?.replica?.papel === "standby" && E?.origem?.replica_atraso_bytes != null ? 1 : 0,
  };
  for (const a of ARESTAS) {
    const r = refs[a.id];
    const oculta = (a.soModo && modo && a.soModo !== modo) || (a.id === "pgo-pgr" && (st.pgr.inativo || E?.replica?.papel === "primario"));
    const quebrada = (st[a.de]?.cls === "erro" || st[a.de]?.cls === "congelado" || st[a.para]?.cls === "erro" || st[a.para]?.cls === "congelado");
    r.g.setAttribute("class", `aresta${oculta ? " oculta" : ""}${quebrada ? " quebrada" : (fluxo[a.id] || 0) > 0 ? " ativa" : ""}`);
    const [txt, alerta, extra] = rot[a.id]();
    r.t.textContent = txt;
    r.t2.textContent = alerta || extra || "";
    r.t2.setAttribute("class", alerta ? "alerta" : "");
  }
}

// ------------------------------------------------------------------ painel de detalhe e ações
function servicosDoNo(chave) {
  if (chave === "kafka") return BROKERS;
  if (chave === "consumidores") return E?.modo ? CONSUMIDORES.map((k) => NOS[k].servico(E.modo)).filter((s) => E?.conteineres?.[s]) : [];
  const no = NOS[chave];
  if (!no?.servico || (!E?.modo && !(no.broker && !no.consumidor) && !["pgo", "pgd", "pgr"].includes(chave))) return [];
  if (no.soCdc && E?.modo !== "cdc") return [];
  return [no.servico(E?.modo || "domain-events")];
}

function selecionar(chave) {
  selecionado = selecionado === chave ? null : chave;
  atualizarDiagrama();
  desenharDetalhe();
}
svg.addEventListener("click", () => { if (selecionado) selecionar(selecionado); });

function linhas(pares) {
  return "<dl>" + pares.filter(Boolean).map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join("") + "</dl>";
}

function desenharDetalhe() {
  const caixa = $("#detalhe");
  if (!selecionado) {
    caixa.innerHTML = `<h2>Nenhum serviço selecionado</h2><p class="dica">Selecione um serviço no diagrama.</p>`;
    caixa.dataset.html = "";
    return;
  }
  const bloqueado = falhasBloqueadas();
  if (selecionado === "cliente") {
    const t = E?.trabalho;
    caixa.dataset.html = "";
    caixa.innerHTML = `<h2>Gerador de carga</h2>` + linhas([
      ["script", t ? esc(t.descricao) : "nenhum"],
      t && ["estado", t.rodando ? "rodando" : `terminou (código ${t.codigo})`],
      ["commits/s", fmt(taxa("api_commits_s"))],
    ]) + `<p class="dica">Dispare carga ou o Cenário 1 em “Scripts”.</p>`;
    return;
  }
  const servicos = servicosDoNo(selecionado);
  const st = selecionado === "kafka" ? estadoFila() : selecionado === "consumidores" ? estadoConsumidores() : estadoNo(selecionado);
  const titulo = selecionado === "kafka" ? "Kafka (3 brokers)" : selecionado === "consumidores" ? "analytics-service (grupo de consumo)"
    : NOS[selecionado].consumidor ? `analytics-service · ${NOS[selecionado].titulo} · :${NOS[selecionado].porta}` : NOS[selecionado].titulo;
  const pares = [["estado", `<span class="pilula"><i class="bolinha ${st.cls}"></i>${esc(st.texto)}</span>`]];
  for (const s of servicos) {
    const c = E?.conteineres?.[s];
    pares.push([s, c ? esc(`${c.status}${c.saude ? ` · ${c.saude}` : ""}`) : "não criado"]);
  }
  const m = E?.metricas || {};
  if (selecionado === "kafka") pares.push(["configuração", "KRaft · fator de replicação 3 · min.insync.replicas 2"]);
  if (selecionado === "origem") {
    const h = E?.healthz?.origem || {};
    pares.push(["/healthz", h.ok ? `ok · ${fmt(h.ms, 1)} ms` : esc(h.erro || "sem resposta")]);
    pares.push(["operações commitadas", fmt(m.operations_committed_total)], ["erros HTTP", fmt(m.http_errors_total)]);
    if (E?.modo === "domain-events") pares.push(["eventos publicados", fmt(m.events_published_total)], ["falhas na publicação síncrona", fmt(m.events_publish_failed_total)],
      ["esperando retentativa", fmt(m.events_retry_pending)], ["confirmados com atraso", fmt(m.events_late_confirmed_total)],
      ["recuperados por retentativa", fmt(m.events_retry_recovered_total)],
      ["declarados perdidos", fmt(m.events_declared_lost_total)]);
  } else if (selecionado === "consumidores") {
    const g = E?.grupo;
    pares.push(["estado no coordenador", g ? esc(g.estado) : "sem resposta"], ["membros", g ? fmt(g.membros) : "–"]);
    pares.push(["consumidas (soma)", fmt(m.messages_consumed_total)], ["aplicadas (soma)", fmt(m.messages_applied_total)],
      ["saídas por max.poll.interval", fmt(m.group_expulsions_total)]);
  } else if (NOS[selecionado]?.consumidor) {
    const c = E?.consumidores?.[NOS[selecionado].consumidor] || {};
    const h = c.healthz || {};
    const mi = c.metricas || {};
    pares.push(["/healthz", h.ok ? `ok · ${fmt(h.ms, 1)} ms` : esc(h.erro || "sem resposta")]);
    pares.push(["consumidas", fmt(mi.messages_consumed_total)], ["aplicadas", fmt(mi.messages_applied_total)],
      ["inválidas", fmt(mi.messages_invalid_total)], ["erros no banco", fmt(mi.db_errors_total)]);
  } else if (selecionado === "pgo") {
    const o = E?.origem || {};
    pares.push(["aceita conexões", o.disponivel ? "sim" : `não (${esc(o.erro || "?")})`]);
    if (o.disponivel) pares.push(["WAL no disco", bytes(o.wal_bytes)]);
    if (o.slot_existe) pares.push(["slot Debezium", o.slot_ativo ? "ativo" : "inativo"], ["WAL retido", bytes(o.slot_retido_bytes)], ["atraso do slot", bytes(o.slot_atraso_bytes)]);
    if (o.replica_atraso_bytes != null) pares.push(["atraso da réplica", bytes(o.replica_atraso_bytes)]);
  } else if (selecionado === "pgr") {
    const r = E?.replica || {};
    pares.push(["papel", r.disponivel ? (r.papel === "primario" ? "primário (promovida)" : "standby, replicação assíncrona") : `sem resposta (${esc(r.erro || "?")})`]);
    if (r.papel === "primario") pares.push(["slot Debezium", r.slot_existe ? (r.slot_ativo ? "recriado, ativo" : "recriado, inativo") : "não existe"]);
  } else if (selecionado === "connect" && E?.conector) {
    pares.push(["tarefa", esc(E.conector.estado)]);
    if (E.conector.erro) pares.push(["erro", esc(E.conector.erro)]);
    const v = E.conector.vigia;
    if (v) pares.push(["reinícios automáticos", fmt(v.connector_auto_restarts_total)],
      ["reinício automático", v.connector_restart_gave_up ? "desistiu: intervenção manual necessária" : `${fmt(v.connector_restart_attempts)} tentativas usadas`]);
  }
  let html = `<h2>${esc(titulo)}</h2>` + linhas(pares);
  const no = NOS[selecionado];
  if (no?.opcional && !servicos.some((s) => E?.conteineres?.[s]) && E?.modo) {
    const dis = bloqueado ? "disabled" : "";
    html += `<p class="dica">${selecionado === "pgr" ? "A réplica só existe no Cenário 2. Subir agora copia a origem atual (pg_basebackup)." : "Instância extra do Cenário 5, no mesmo grupo de consumo."}</p>
      <div class="botoes"><button data-acao-no="subir" ${dis}>Subir</button></div>`;
  } else if (servicos.length) {
    const algumCongelado = servicos.some((s) => E?.conteineres?.[s]?.estado === "paused");
    const dis = bloqueado ? "disabled" : "";
    html += `<div class="botoes">
      <button class="perigo" data-acao-no="stop" ${dis}>Parar</button>
      <button class="perigo" data-acao-no="kill" ${dis}>Matar</button>
      <button class="perigo" data-acao-no="pause" ${dis}>Congelar</button>
      <button data-acao-no="${algumCongelado ? "unpause" : "start"}" ${dis}>Religar</button>
      <button data-acao-no="restart" ${dis}>Reiniciar</button></div>`;
    if (selecionado === "connect") html += `<div class="botoes"><button data-acao="conector" ${dis}>Reiniciar tarefa do conector</button></div>`;
    if (selecionado === "pgr" && E?.replica?.papel === "standby") html += `<div class="botoes"><button class="perigo" data-acao="failover" ${dis}>Failover: derrubar o primário e promover</button></div>`;
  } else {
    html += `<p class="dica">Não há contêiner deste serviço no ar.</p>`;
  }
  if (caixa.dataset.html === html) return;  // evita recriar os botões a cada segundo
  caixa.dataset.html = html;
  caixa.innerHTML = html;
  caixa.querySelectorAll("[data-acao-no]").forEach((b) => b.addEventListener("click", () => injetar(servicos, b.dataset.acaoNo)));
  caixa.querySelectorAll("[data-acao=conector]").forEach((b) => b.addEventListener("click", reiniciarConector));
  caixa.querySelectorAll("[data-acao=failover]").forEach((b) => b.addEventListener("click", failover));
}

function falhasBloqueadas() {
  const t = E?.trabalho;
  return t?.rodando && (t.tipo.startsWith("cenario") || t.tipo === "ambiente") ? `“${t.descricao}” está em andamento e controla os contêineres.` : null;
}

async function acao(fn) {
  const aviso = $("#aviso-falha");
  aviso.hidden = true;
  try { await fn(); } catch (e) { aviso.textContent = e.message; aviso.hidden = false; }
  atualizar();
}
const injetar = (servicos, acaoDocker) => acao(() => api("/api/falha", { servicos, acao: acaoDocker }));
const reiniciarConector = () => acao(() => api("/api/conector/reiniciar", {}));
const failover = () => {
  if (confirm("Derrubar o primário (SIGKILL) e promover a réplica? Depois disso o primário antigo fica parado até subir o backend de novo.")) {
    acao(() => api("/api/failover", {}));
  }
};

const PRESETS = {
  fila: () => BROKERS, quorum: () => ["kafka-2", "kafka-3"], broker: () => ["kafka-3"],
  consumidor: () => (E?.modo ? [`analytics-service-${E.modo}`] : []),
  instancia: () => (E?.modo && E?.conteineres?.[`analytics-service-${E.modo}-2`] ? [`analytics-service-${E.modo}-2`] : []),
  origem: () => ["postgres-origem"], destino: () => ["postgres-destino"], connect: () => ["kafka-connect"],
};
document.querySelectorAll("[data-preset]").forEach((b) => b.addEventListener("click", () => {
  const servicos = PRESETS[b.dataset.preset]();
  if (servicos.length) injetar(servicos, $("#parada").value);
}));
document.querySelectorAll(".lateral [data-acao=conector]").forEach((b) => b.addEventListener("click", reiniciarConector));
document.querySelectorAll(".lateral [data-acao=failover]").forEach((b) => b.addEventListener("click", failover));
document.querySelectorAll("[data-subir]").forEach((b) => b.addEventListener("click", () => {
  if (!E?.modo) return;
  const servicos = b.dataset.subir === "replica" ? ["postgres-origem-replica"] : [`analytics-service-${E.modo}-2`, `analytics-service-${E.modo}-3`];
  injetar(servicos, "subir");
}));
$("#restaurar").addEventListener("click", () => acao(() => api("/api/restaurar", {})));

function atualizarControles() {
  const bloqueio = falhasBloqueadas();
  document.querySelectorAll(".lateral button").forEach((b) => { b.disabled = !!bloqueio; });
  document.querySelectorAll(".so-cdc").forEach((b) => { b.hidden = E?.modo !== "cdc"; });
  const replica = E?.replica?.disponivel ? E.replica.papel : null;
  const extras = !!(E?.modo && E?.conteineres?.[`analytics-service-${E.modo}-2`]);
  document.querySelectorAll("[data-mostra]").forEach((b) => {
    const r = b.dataset.mostra;
    b.hidden = !E?.modo || (r === "sem-replica" ? !!E?.conteineres?.["postgres-origem-replica"] : r === "standby" ? replica !== "standby"
      : r === "sem-extras" ? extras : r === "extras" ? !extras : false);
  });
  const aviso = $("#aviso-falha");
  if (bloqueio) { aviso.textContent = `Falhas manuais desativadas: ${bloqueio}`; aviso.hidden = false; }
  else if (aviso.textContent.startsWith("Falhas manuais")) aviso.hidden = true;
  const rodando = !!E?.trabalho?.rodando;
  document.querySelectorAll(".scripts form button, [data-ambiente]").forEach((b) => { b.disabled = rodando; });
}

// ------------------------------------------------------------------ scripts
function iniciarTrabalho(tipo, parametros) {
  return acao(async () => {
    try { await api("/api/trabalho", { tipo, parametros }); }
    catch (e) { alert(e.message); }
  });
}
document.querySelectorAll("[data-ambiente]").forEach((b) => b.addEventListener("click", () => {
  if (confirm("Isso derruba o ambiente atual e zera as bases. Continuar?")) iniciarTrabalho("ambiente", { modo: b.dataset.ambiente });
}));
const formCarga = $("#form-carga");
formCarga.perfil.addEventListener("change", () => {
  const lote = formCarga.perfil.value !== "constante";
  formCarga.querySelectorAll("[data-perfil=constante]").forEach((l) => { l.hidden = lote; });
  formCarga.querySelectorAll("[data-perfil=lote]").forEach((l) => { l.hidden = !lote; });
});
formCarga.addEventListener("submit", (ev) => {
  ev.preventDefault();
  iniciarTrabalho("carga", Object.fromEntries(new FormData(formCarga)));
});
const formC1 = $("#form-cenario1");
formC1.addEventListener("submit", (ev) => {
  ev.preventDefault();
  const fd = new FormData(formC1);
  const p = Object.fromEntries(fd);
  p.brokers = fd.getAll("brokers");
  p.reiniciar_conector_falho = fd.has("reiniciar_conector_falho");
  p.sem_recuperacao = fd.has("sem_recuperacao");
  if (confirm("O Cenário 1 reinicializa o ambiente (down -v) antes de cada execução. Continuar?")) iniciarTrabalho("cenario1", p);
});
// Formulários dos cenários 2, 3 e 5: os campos têm os mesmos nomes dos parâmetros do servidor.
for (const [id, tipo, aviso] of [
  ["#form-cenario2", "cenario2", "O Cenário 2 reinicializa o ambiente (down -v), sobe a réplica e derruba o primário. Continuar?"],
  ["#form-cenario3", "cenario3", "O Cenário 3 reinicializa o ambiente (down -v) e mata o componente de propagação em ciclos. Continuar?"],
  ["#form-cenario5", "cenario5", "O Cenário 5 reinicializa o ambiente (down -v) com três instâncias consumidoras. Continuar?"],
]) {
  const f = $(id);
  f.addEventListener("submit", (ev) => {
    ev.preventDefault();
    const fd = new FormData(f);
    const p = Object.fromEntries(fd);
    for (const c of f.querySelectorAll("input[type=checkbox]")) p[c.name] = c.checked;
    if (ev.submitter?.dataset.calibrar) p.calibrar = true;
    if (confirm(aviso)) iniciarTrabalho(tipo, p);
  });
}

$("#trabalho-parar").addEventListener("click", () => {
  if (confirm("Interromper o script? Se a falha estiver ativa, use “Restaurar tudo” depois.")) acao(() => api("/api/trabalho/parar", {}));
});

function desenharTrabalho() {
  const t = E?.trabalho;
  const caixa = $("#trabalho");
  if (!t) { caixa.hidden = true; return; }
  caixa.hidden = false;
  $("#trabalho-desc").textContent = t.descricao;
  const dur = Math.round(((t.fim || Date.now() / 1000) - t.inicio));
  $("#trabalho-info").textContent = `${t.rodando ? "rodando há" : `terminou (código ${t.codigo}) após`} ${dur}s · ${t.comando}`;
  $("#trabalho-parar").hidden = !t.rodando;
  const pre = $("#trabalho-saida");
  const noFim = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 20;
  pre.textContent = t.saida.join("\n");
  if (noFim) pre.scrollTop = pre.scrollHeight;
}

// ------------------------------------------------------------------ linha do tempo
function desenharEventos(novos) {
  if (!novos.length) return;
  eventos.push(...novos);
  ultimoEvento = eventos[eventos.length - 1].t;
  while (eventos.length > 300) eventos.shift();
  $("#eventos").innerHTML = eventos.slice().reverse().map((e) =>
    `<li class="ev-${esc(e.tipo)}"><span class="hora">${esc(e.hora)}</span><i class="bolinha"></i><span class="txt">${esc(e.texto)}</span></li>`).join("");
}

// ------------------------------------------------------------------ gráfico
const SERIES = [
  { chave: "api_commits_s", nome: "commits na origem", cor: "var(--s1)" },
  { chave: "events_published_s", nome: "eventos publicados", cor: "var(--s2)", modo: "domain-events" },
  { chave: "messages_consumed_s", nome: "consumidas no destino", cor: "var(--s3)" },
  { chave: "events_publish_failed_s", nome: "falhas de publicação", cor: "var(--erro)", modo: "domain-events" },
];
const JANELA = 300;
const graf = $("#grafico");
let geom = null;

function seriesAtivas() { return SERIES.filter((s) => !s.modo || s.modo === E?.modo); }

function desenharGrafico() {
  const hist = (E?.historico || []);
  const larg = graf.clientWidth || 600, alt = 220;
  graf.setAttribute("viewBox", `0 0 ${larg} ${alt}`);
  graf.innerHTML = "";
  const m = { e: 44, d: 12, t: 10, b: 24 };
  const agora = E?.t || Date.now() / 1000;
  const t0 = agora - JANELA;
  const pts = hist.filter((h) => h.t >= t0);
  const ativas = seriesAtivas();
  let max = 0;
  for (const h of pts) for (const s of ativas) max = Math.max(max, h[s.chave] || 0);
  const passo = max <= 5 ? 1 : Math.pow(10, Math.floor(Math.log10(max))) * (max / Math.pow(10, Math.floor(Math.log10(max))) > 5 ? 2 : 1);
  const ymax = Math.max(5, Math.ceil(max / passo) * passo);
  const x = (t) => m.e + ((t - t0) / JANELA) * (larg - m.e - m.d);
  const y = (v) => alt - m.b - (v / ymax) * (alt - m.t - m.b);
  geom = { x, y, pts, m, larg, alt, t0 };

  // faixas em que algum serviço estava fora do ar
  let ini = null;
  pts.forEach((h, i) => {
    const fora = (h.fora || []).length > 0;
    if (fora && ini == null) ini = h.t;
    if ((!fora || i === pts.length - 1) && ini != null) {
      const fim = fora ? h.t : h.t;
      el("rect", { class: "faixa-falha", x: x(ini), y: m.t, width: Math.max(2, x(fim) - x(ini)), height: alt - m.t - m.b }, graf);
      ini = null;
    }
  });
  for (let v = 0; v <= ymax; v += ymax / 4) {
    el("line", { class: v === 0 ? "base" : "grade-linha", x1: m.e, x2: larg - m.d, y1: y(v), y2: y(v) }, graf);
    const t = el("text", { x: m.e - 6, y: y(v) + 4, "text-anchor": "end" }, graf);
    t.textContent = fmt(v);
  }
  for (let s = 0; s <= JANELA; s += larg < 520 ? 150 : 60) {
    const t = el("text", { x: x(t0 + s), y: alt - 6, "text-anchor": s === 0 ? "start" : s === JANELA ? "end" : "middle" }, graf);
    t.textContent = s === JANELA ? "agora" : `-${(JANELA - s) / 60} min`;
  }
  for (const s of ativas) {
    let d = "";
    let caneta = false;
    for (const h of pts) {
      const v = h[s.chave];
      if (v == null) { caneta = false; continue; }
      d += `${caneta ? "L" : "M"}${x(h.t).toFixed(1)} ${y(v).toFixed(1)} `;
      caneta = true;
    }
    if (d) el("path", { class: "serie", d, stroke: s.cor }, graf);
  }
  $("#legenda-grafico").innerHTML = ativas.map((s) => `<span><i class="traco" style="background:${s.cor}"></i>${esc(s.nome)}</span>`).join("");
}

graf.addEventListener("mousemove", (ev) => {
  if (!geom || !geom.pts.length) return;
  const r = graf.getBoundingClientRect();
  const px = (ev.clientX - r.left) * (geom.larg / r.width);
  let melhor = geom.pts[0];
  for (const h of geom.pts) if (Math.abs(geom.x(h.t) - px) < Math.abs(geom.x(melhor.t) - px)) melhor = h;
  graf.querySelector(".cursor")?.remove();
  el("line", { class: "cursor", x1: geom.x(melhor.t), x2: geom.x(melhor.t), y1: geom.m.t, y2: geom.alt - geom.m.b }, graf);
  const tip = $("#tooltip");
  const hora = new Date(melhor.t * 1000).toLocaleTimeString("pt-BR");
  tip.innerHTML = `<div class="t">${hora}</div>` + seriesAtivas().map((s) =>
    `<div class="l"><span><i class="traco" style="background:${s.cor}"></i>${esc(s.nome)}</span><span>${fmt(melhor[s.chave], 1)}</span></div>`).join("") +
    ((melhor.fora || []).length ? `<div class="l"><span>fora do ar</span><span>${esc(melhor.fora.join(", "))}</span></div>` : "");
  tip.hidden = false;
  const caixa = graf.parentElement.getBoundingClientRect();
  let left = ev.clientX - caixa.left + 14;
  if (left + 220 > caixa.width) left = ev.clientX - caixa.left - 230;
  tip.style.left = `${Math.max(0, left)}px`;
  tip.style.top = "8px";
});
graf.addEventListener("mouseleave", () => { $("#tooltip").hidden = true; graf.querySelector(".cursor")?.remove(); });

// ------------------------------------------------------------------ ciclo de atualização
async function atualizar() {
  try {
    E = await api(`/api/estado?eventos_desde=${ultimoEvento}`);
    $("#conexao").classList.remove("off");
    $("#conexao").title = "Painel conectado";
  } catch {
    $("#conexao").classList.add("off");
    $("#conexao").title = "Sem resposta do servidor do painel";
    return;
  }
  const modo = $("#modo");
  if (E.conteineres?._erro) modo.textContent = "Docker indisponível";
  else modo.textContent = E.modo ? ROTULO_MODO[E.modo] : "nenhum backend no ar";
  if (E.conteineres?._erro) modo.title = E.conteineres._erro.mensagem;
  atualizarDiagrama();
  desenharDetalhe();
  atualizarControles();
  desenharTrabalho();
  desenharEventos(E.eventos || []);
  desenharGrafico();
}

montarDiagrama();
atualizar();
setInterval(atualizar, 1000);
window.addEventListener("resize", desenharGrafico);
