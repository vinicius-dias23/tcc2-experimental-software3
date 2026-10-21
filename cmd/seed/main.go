// seed faz a carga inicial dos bancos a partir do dataset RetailRocket preparado por
// scripts/dados/preparar_retailrocket.py (operacoes.csv, itens.csv e categorias.csv).
//
// As linhas de operacoes.csv, já em ordem de horário, recebem um identificador sequencial
// (E000000001, E000000002, ...). Das primeiras RETAILROCKET_MAX_OPS linhas (0 = todas), a
// primeira fração (SEED_FRACTION, padrão 0.7) é gravada diretamente nas bases de origem e de
// destino, que partem assim do mesmo estado. A fração restante não é tocada aqui: ela é
// reproduzida como carga transacional pela API (scripts/carga), com a mesma regra de corte
// implementada em scripts/comum/retailrocket.py.
//
// Os agregados seguem a mesma regra do tracking-service: o catálogo inicial (itens.csv) entra
// com versão 1; cada evento de comportamento soma 1 ao contador do tipo e à versão do
// visitante e do item (criando-os com versão 1 se forem novos); cada mudança de propriedade
// grava o valor no item e soma 1 à versão.
//
// A carga é idempotente: se a origem já tiver eventos, nada é feito.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/model"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/pg"
)

// Arquivos gerados por scripts/dados/preparar_retailrocket.py e marcador da amostra sintética,
// gravado por scripts/dados/gerar_amostra_sintetica.py.
const (
	operationsFile  = "operacoes.csv"
	itemsFile       = "itens.csv"
	categoriesFile  = "categorias.csv"
	syntheticMarker = "_SINTETICO"
)

var operationColumns = []string{"timestamp", "event", "visitorid", "itemid", "transactionid", "property", "value"}

// aggregate é o estado de um visitante ou de um item durante a reprodução.
type aggregate struct {
	views, carts, purchases int64
	version                 int64
	lastEvent               int // posição da última operação que o tocou (0 = nenhuma)
	category, available     string
}

func (a *aggregate) count(eventType string) {
	switch eventType {
	case model.TypeView:
		a.views++
	case model.TypeAddToCart:
		a.carts++
	case model.TypeTransaction:
		a.purchases++
	}
}

func main() {
	ctx := context.Background()
	dir := config.String("RETAILROCKET_DATA_DIR", "/data")
	fraction, err := strconv.ParseFloat(config.String("SEED_FRACTION", "0.7"), 64)
	if err != nil || fraction < 0 || fraction > 1 {
		log.Fatalf("SEED_FRACTION deve estar entre 0 e 1")
	}
	maxOps := config.Int("RETAILROCKET_MAX_OPS", 0)
	for _, f := range []string{operationsFile, itemsFile, categoriesFile} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			log.Fatalf("%s não encontrado em %s. Baixe o dataset do Kaggle e rode "+
				"scripts/dados/preparar_retailrocket.py (ver README), ou gere a amostra sintética com "+
				"scripts/dados/gerar_amostra_sintetica.py e prepare-a.", f, dir)
		}
	}
	_, statErr := os.Stat(filepath.Join(dir, syntheticMarker))
	synthetic := statErr == nil
	if synthetic {
		log.Printf("ATENÇÃO: %s contém a amostra SINTÉTICA, não o dataset real do RetailRocket", dir)
	}

	origin := pg.Connect(ctx, config.String("ORIGIN_DATABASE_URL", "postgres://retailrocket:retailrocket@postgres-origem:5432/retailrocket"), 4)
	defer origin.Close()
	dest := pg.Connect(ctx, config.String("DESTINATION_DATABASE_URL", "postgres://retailrocket:retailrocket@postgres-destino:5432/retailrocket"), 4)
	defer dest.Close()

	var existing int
	if err := origin.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&existing); err != nil {
		log.Fatal(err)
	}
	if existing > 0 {
		log.Printf("origem já possui %d eventos; carga inicial ignorada", existing)
		return
	}

	start := time.Now()
	opsPath := filepath.Join(dir, operationsFile)
	total := countRows(opsPath)
	if maxOps > 0 && maxOps < total {
		total = maxOps
	}
	cut := int(math.Floor(float64(total) * fraction))
	log.Printf("%s: %d operações consideradas (RETAILROCKET_MAX_OPS=%d); %d (%.0f%%) entram na carga inicial",
		operationsFile, total, maxOps, cut, fraction*100)

	tmp, err := os.MkdirTemp("", "seed")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	evFile, visFile := filepath.Join(tmp, "events.csv"), filepath.Join(tmp, "visitors.csv")
	itFile, catFile := filepath.Join(tmp, "items.csv"), filepath.Join(tmp, "categories.csv")

	items := readCatalog(filepath.Join(dir, itemsFile))
	catalog := len(items)
	visitors := replay(opsPath, cut, evFile, items)
	writeVisitors(visitors, visFile)
	writeItems(items, itFile)
	categories := copyCategories(filepath.Join(dir, categoriesFile), catFile)
	log.Printf("%d operações, %d visitantes, %d itens (%d do catálogo inicial) e %d categorias preparados em %s",
		cut, len(visitors), len(items), catalog, categories, time.Since(start).Round(time.Millisecond))

	for _, db := range []*pgxpool.Pool{origin, dest} {
		copyFile(ctx, db, catFile, "categories", "category_id, parent_id")
		copyFile(ctx, db, itFile, "items", "item_id, category_id, available, views, add_to_carts, purchases, version, last_event_id")
		copyFile(ctx, db, visFile, "visitors", "visitor_id, views, add_to_carts, purchases, version, last_event_id")
		copyFile(ctx, db, evFile, "events", "event_id, occurred_at, event_type, visitor_id, item_id, transaction_id, property, value")
	}

	if _, err := origin.Exec(ctx, `INSERT INTO seed_info (seed_fraction, data_file, operations_total,
		operations_seeded, visitors_seeded, items_seeded, categories_seeded, synthetic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		fraction, operationsFile, total, cut, len(visitors), len(items), categories, synthetic); err != nil {
		log.Fatal(err)
	}
	for _, db := range []*pgxpool.Pool{origin, dest} {
		if _, err := db.Exec(ctx, `ANALYZE`); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("carga inicial concluída em %s", time.Since(start).Round(time.Millisecond))
}

// countRows conta as linhas de dados (sem o cabeçalho).
func countRows(path string) int {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	n, last := 0, byte('\n')
	for {
		k, err := f.Read(buf)
		n += bytes.Count(buf[:k], []byte{'\n'})
		if k > 0 {
			last = buf[k-1]
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if last != '\n' {
		n++ // última linha sem quebra
	}
	return n - 1
}

// openCSV abre um CSV e devolve o leitor (já depois do cabeçalho) e o índice das colunas.
func openCSV(path string, want []string) (*os.File, *csv.Reader, map[string]int) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
	r.ReuseRecord = true
	header, err := r.Read()
	if err != nil {
		log.Fatalf("%s: %v", path, err)
	}
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.TrimPrefix(strings.TrimSpace(h), "\xef\xbb\xbf")] = i
	}
	for _, c := range want {
		if _, ok := idx[c]; !ok {
			log.Fatalf("%s: coluna %s ausente (esperado: %s)", path, c, strings.Join(want, ","))
		}
	}
	return f, r, idx
}

// readCatalog lê o catálogo inicial: cada item com versão 1.
func readCatalog(path string) map[int64]*aggregate {
	f, r, idx := openCSV(path, []string{"itemid", "categoryid", "available"})
	defer f.Close()
	items := map[int64]*aggregate{}
	for line := 2; ; line++ {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("%s: linha %d: %v", path, line, err)
		}
		id := parseInt(rec[idx["itemid"]], path, line)
		items[id] = &aggregate{version: 1, category: rec[idx["categoryid"]], available: rec[idx["available"]]}
	}
	return items
}

// replay percorre as primeiras `cut` operações aplicando as regras do tracking-service,
// grava os eventos num CSV pronto para o COPY e devolve o estado final dos visitantes. O
// estado dos itens é atualizado em `items`.
func replay(path string, cut int, out string, items map[int64]*aggregate) map[int64]*aggregate {
	f, r, idx := openCSV(path, operationColumns)
	defer f.Close()
	w, err := os.Create(out)
	if err != nil {
		log.Fatal(err)
	}
	bw := bufio.NewWriterSize(w, 1<<20)
	visitors := make(map[int64]*aggregate, cut/2)
	get := func(m map[int64]*aggregate, id int64) *aggregate {
		a, ok := m[id]
		if !ok {
			a = &aggregate{}
			m[id] = a
		}
		a.version++
		return a
	}
	for i := 1; i <= cut; i++ {
		rec, err := r.Read()
		if err != nil {
			log.Fatalf("%s: linha %d: %v", path, i+1, err)
		}
		col := func(c string) string { return rec[idx[c]] }
		typ := col("event")
		ts := parseInt(col("timestamp"), path, i+1)
		item := parseInt(col("itemid"), path, i+1)
		occurred := time.UnixMilli(ts).UTC().Format("2006-01-02 15:04:05.000+00")
		switch {
		case model.BehaviorTypes[typ]:
			v := get(visitors, parseInt(col("visitorid"), path, i+1))
			it := get(items, item)
			v.count(typ)
			it.count(typ)
			v.lastEvent, it.lastEvent = i, i
			fmt.Fprintf(bw, "%s,%s,%s,%s,%d,%s,,\n", eventID(i), occurred, typ, col("visitorid"), item, col("transactionid"))
		case typ == model.TypeProperty:
			it := get(items, item)
			switch col("property") {
			case model.PropertyCategory:
				it.category = col("value")
			case model.PropertyAvailable:
				it.available = col("value")
			default:
				log.Fatalf("%s: linha %d: propriedade desconhecida %q", path, i+1, col("property"))
			}
			it.lastEvent = i
			fmt.Fprintf(bw, "%s,%s,%s,,%d,,%s,%s\n", eventID(i), occurred, typ, item, col("property"), col("value"))
		default:
			log.Fatalf("%s: linha %d: tipo desconhecido %q", path, i+1, typ)
		}
	}
	if err := bw.Flush(); err != nil {
		log.Fatal(err)
	}
	w.Close()
	return visitors
}

func sortedKeys(m map[int64]*aggregate) []int64 {
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func lastEvent(a *aggregate) string {
	if a.lastEvent == 0 {
		return ""
	}
	return eventID(a.lastEvent)
}

func writeVisitors(visitors map[int64]*aggregate, out string) {
	w, err := os.Create(out)
	if err != nil {
		log.Fatal(err)
	}
	bw := bufio.NewWriterSize(w, 1<<20)
	for _, id := range sortedKeys(visitors) {
		v := visitors[id]
		fmt.Fprintf(bw, "%d,%d,%d,%d,%d,%s\n", id, v.views, v.carts, v.purchases, v.version, lastEvent(v))
	}
	if err := bw.Flush(); err != nil {
		log.Fatal(err)
	}
	w.Close()
}

func writeItems(items map[int64]*aggregate, out string) {
	w, err := os.Create(out)
	if err != nil {
		log.Fatal(err)
	}
	bw := bufio.NewWriterSize(w, 1<<20)
	for _, id := range sortedKeys(items) {
		it := items[id]
		available := ""
		switch it.available {
		case "1":
			available = "true"
		case "0":
			available = "false"
		}
		fmt.Fprintf(bw, "%d,%s,%s,%d,%d,%d,%d,%s\n", id, it.category, available, it.views, it.carts, it.purchases,
			it.version, lastEvent(it))
	}
	if err := bw.Flush(); err != nil {
		log.Fatal(err)
	}
	w.Close()
}

func copyCategories(path, out string) int {
	f, r, idx := openCSV(path, []string{"categoryid", "parentid"})
	defer f.Close()
	w, err := os.Create(out)
	if err != nil {
		log.Fatal(err)
	}
	bw := bufio.NewWriter(w)
	n := 0
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("%s: %v", path, err)
		}
		fmt.Fprintf(bw, "%s,%s\n", rec[idx["categoryid"]], rec[idx["parentid"]])
		n++
	}
	if err := bw.Flush(); err != nil {
		log.Fatal(err)
	}
	w.Close()
	return n
}

func copyFile(ctx context.Context, db *pgxpool.Pool, path, table, cols string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	conn, err := db.Acquire(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Release()
	tag, err := conn.Conn().PgConn().CopyFrom(ctx, f, fmt.Sprintf("COPY %s (%s) FROM STDIN WITH (FORMAT csv)", table, cols))
	if err != nil {
		log.Fatalf("copiando %s: %v", table, err)
	}
	log.Printf("%-10s %9d linhas (%s)", table, tag.RowsAffected(), db.Config().ConnConfig.Host)
}

// eventID é o identificador da operação a partir da posição da linha em operacoes.csv
// (1 = primeira). Precisa coincidir com scripts/comum/retailrocket.py.
func eventID(n int) string { return fmt.Sprintf("E%09d", n) }

func parseInt(v, path string, line int) int64 {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Fatalf("%s: linha %d: inteiro inválido %q", path, line, v)
	}
	return n
}
