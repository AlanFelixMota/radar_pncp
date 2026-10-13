"""Regressao offline dos fluxos afetados pela nova identidade e pela migracao."""
from contextlib import closing
from pathlib import Path
import atexit
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "radar_versoes" / "atual"
ISOLADO = tempfile.TemporaryDirectory(prefix="radar_teste_")
atexit.register(ISOLADO.cleanup)
os.environ.update(RADAR_DATA_DIR=ISOLADO.name, RADAR_TEST_MODE="1")
sys.path[:0] = [str(BASE), str(ROOT)]

import buscas_salvas
import cestas
import coletar_pncp
import coleta_scheduler
import exportar_cesta
import fts_sync
import job_manager
import migrar_dados
import monitoramento
import pacotes_radar_pncp
import relatorio_cesta


class Fluxos(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=ISOLADO.name)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "compras.db"
        con = coletar_pncp.conectar(self.db)
        coletar_pncp.criar_banco(con)
        con.close()
        self.pesquisas = self.root / "radar_pesquisas.db"
        self.jobs = self.root / "radar_jobs.db"
        cestas.preparar(self.pesquisas)
        job_manager.preparar_banco_jobs(self.jobs)

    def inserir(self, seq):
        ident = f"12345678000190-1-{seq:06d}/2026"
        with closing(sqlite3.connect(self.db)) as con:
            con.execute("INSERT INTO contratacoes(numero_controle_pncp,objeto,data_publicacao_pncp,orgao_razao_social,primeira_coleta_em,ultima_coleta_em) VALUES(?,?,?,?,?,?)",
                        (ident, "Impressora monocromatica", "2026-10-01", "Orgao de teste", "2026-10-01", "2026-10-01"))
            con.commit()
        return ident

    def test_monitoramento_baseline_e_novidades(self):
        self.inserir(1)
        ident = buscas_salvas.salvar(self.pesquisas, "Impressoras", "contratacoes", {"texto": "Impressora"}, 20)
        busca = buscas_salvas.obter(self.pesquisas, ident)
        monitor = monitoramento.iniciar(self.db, self.pesquisas, ident, revisao_busca=busca["revisao"])["id"]
        self.inserir(2)
        resultado = monitoramento.verificar(self.db, self.pesquisas, monitor)
        self.assertEqual(resultado["novas"], 1)
        self.assertEqual(resultado["total"], 2)
        self.assertEqual(monitoramento.listar(self.pesquisas)[0]["pendentes"], 1)
        self.assertEqual(monitoramento.verificar(self.db, self.pesquisas, monitor)["novas"], 0)

    def test_indice_fts_preserva_busca(self):
        self.inserir(1)
        indice = self.root / "radar_fts.db"
        fts_sync.sincronizar(self.db, indice, modo="build")
        with closing(sqlite3.connect(indice)) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM contratacoes_fts WHERE contratacoes_fts MATCH 'Impressora'").fetchone()[0], 1)

    def test_checkpoints_e_nomes_de_agendas(self):
        job_manager.salvar_checkpoint_coleta("2026-10-01", "2026-10-03", 6, "Pregao", "PARCIAL", 2, 4, None, self.jobs)
        row = job_manager.obter_checkpoint_coleta("2026-10-01", "2026-10-03", 6, self.jobs)
        self.assertEqual(row["proxima_pagina"], 2)
        self.assertEqual(coleta_scheduler.TASK_DIARIA, "RADAR_PNCP_COLETA_DIARIA")

    def test_coleta_de_um_dia_e_atualizacao_de_janela_sao_distintas(self):
        import job_worker
        valores = [v for k, v in vars(job_manager).items() if k.startswith("TIPO_")]
        self.assertEqual(len(valores), len(set(valores)))
        jid = job_manager.criar_job_coleta_diaria("2026-10-01", self.db, self.jobs, self.root / "logs")
        job = job_manager.obter_job(jid, self.jobs)
        comando = job_worker.comando_job(job, self.jobs)
        self.assertIn(str(BASE / "radar_diario.py"), comando)
        self.assertIn("--data", comando)
        with closing(sqlite3.connect(self.jobs)) as con:
            con.execute("UPDATE jobs SET status='CONCLUIDA' WHERE id=?", (jid,))
            con.commit()
        jid = job_manager.criar_job_atualizacao_diaria(3, "2026-10-03", self.db, self.jobs, self.root / "logs")
        job = job_manager.obter_job(jid, self.jobs)
        self.assertEqual(job["tipo"], "ATUALIZACAO_DIARIA")
        comando = job_worker.comando_job(job, self.jobs)
        self.assertIn(str(BASE / "coleta_diaria.py"), comando)
        self.assertIn("--dias", comando)

    def test_exportacoes_preservam_precos_e_identidade(self):
        pid = cestas.criar(self.pesquisas, "Impressoras", "Pesquisa de preco")
        pesquisa = cestas.obter(self.pesquisas, pid)
        refs = [{"id": 1, "pesquisa_id": pid, "situacao": "Incluir", "grupo": "Impressora",
                 "justificativa": "Referencia comparavel", "observacoes": "", "selecionado_em": "2026-10-01",
                 "dados": {"tipo_preco": "Homologado", "unidade_medida": "UN", "valor_unitario": 123.4567,
                           "numero_controle_pncp": "12345678000190-1-000001/2026", "numero_item": 1,
                           "descricao_item": "Impressora", "orgao_razao_social": "Orgao de teste",
                           "url_pncp": "https://pncp.gov.br/app/editais/12345678000190/2026/1"}}]
        xlsx = exportar_cesta.gerar_xlsx(pesquisa, refs)
        docx = relatorio_cesta.gerar_docx(pesquisa, refs)
        with zipfile.ZipFile(io.BytesIO(xlsx)) as z:
            self.assertIsNone(z.testzip())
            self.assertIn(b"123.4567", z.read("xl/worksheets/sheet1.xml"))
        with zipfile.ZipFile(io.BytesIO(docx)) as z:
            self.assertIsNone(z.testzip())
            self.assertIn(b"Radar PNCP", z.read("word/header1.xml"))
            self.assertIn(b"Radar PNCP", z.read("docProps/core.xml"))

    def legado(self):
        # Simula prefixos arbitrarios de uma geracao anterior; nao grava marcas.
        ident = self.inserir(1)
        import pesquisa_atualizacao
        import itens_incremental
        with closing(sqlite3.connect(self.db)) as con:
            pesquisa_atualizacao.iniciar(con, "2026-10-01", "2026-10-03")
            pesquisa_atualizacao.registrar(con, "2026-10-01", "2026-10-03", [ident])
            itens_incremental.enfileirar_ids(con, [ident], "LEGADO_MOTOR_NOVA", prioridade=10)
            for suffix in ["janelas_pesquisa", "alvos_pesquisa"]:
                con.execute(f"ALTER TABLE coleta_{suffix} RENAME TO legado_motor_{suffix}")
            con.execute("ALTER TABLE radar_pncp_migracoes RENAME TO legado_app_migracoes")
            con.commit()
        with closing(sqlite3.connect(self.jobs)) as con:
            con.execute("ALTER TABLE coleta_atualizacao_checkpoints RENAME TO legado_motor_atualizacao_checkpoints")
            con.execute("INSERT INTO jobs(tipo,criado_em,status,db_compras) VALUES('LEGADO_MOTOR_DIARIA','2026-10-01','CONCLUIDA','origem/compras.db')")
            con.commit()
        with closing(sqlite3.connect(self.pesquisas)) as con:
            con.execute("CREATE TABLE legado_app_identificadores(tipo TEXT PRIMARY KEY,ultimo_id INTEGER NOT NULL)")
            con.execute("INSERT INTO legado_app_identificadores VALUES('legado_monitor_acompanhamentos',40)")
            con.execute("PRAGMA foreign_keys=ON")
            monitoramento.preparar(con)
            for suffix in ["acompanhamentos", "conhecidos", "ciclos", "novidades"]:
                con.execute(f"ALTER TABLE monitoramento_{suffix} RENAME TO legado_monitor_{suffix}")
            con.execute("CREATE INDEX idx_legado_monitor_teste ON legado_monitor_acompanhamentos(ativo)")
            con.commit()

    def test_migracao_por_copia_preserva_origem_e_contadores(self):
        self.legado()
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.glob("*.db")}
        destino = self.root.parent / (self.root.name + "_copia")
        self.addCleanup(lambda: __import__("shutil").rmtree(destino, ignore_errors=True))
        migrar_dados.migrar(self.root, destino, progresso=lambda _: None)
        self.assertEqual(hashes, {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.glob("*.db")})
        with closing(sqlite3.connect(destino / "radar_jobs.db")) as con:
            self.assertEqual(con.execute("SELECT tipo FROM jobs").fetchone()[0], "ATUALIZACAO_DIARIA")
            self.assertEqual(con.execute("SELECT db_compras FROM jobs").fetchone()[0], str(destino / "compras.db"))
        with closing(sqlite3.connect(destino / "compras.db")) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM coleta_alvos_pesquisa").fetchone()[0], 1)
            self.assertEqual(con.execute("SELECT origem,prioridade FROM itens_fila").fetchone(), ("COLETA_NOVA", 10))
        with cestas.conectar(destino / "radar_pesquisas.db") as con:
            self.assertEqual(con.execute("SELECT tipo,ultimo_id FROM radar_pncp_identificadores").fetchone()[:], ("monitoramento_acompanhamentos", 40))
            self.assertEqual(list(con.execute("PRAGMA foreign_key_check")), [])
            from identificadores import proximo
            self.assertEqual(proximo(con, "monitoramento_acompanhamentos"), 41)

    def test_migracao_recusa_destino_ocupado(self):
        destino = self.root / "outra_pasta"
        destino.mkdir()
        (destino / "nao_apagar.txt").write_text("preservar")
        with self.assertRaises(ValueError):
            migrar_dados.migrar(self.root, destino, progresso=lambda _: None)
        self.assertEqual((destino / "nao_apagar.txt").read_text(), "preservar")

    def test_migracao_recusa_jobs_ativos(self):
        with closing(sqlite3.connect(self.jobs)) as con:
            con.execute("INSERT INTO jobs(tipo,criado_em,status,db_compras) VALUES('COLETA_DIARIA','2026-10-01','EXECUTANDO','teste.db')")
            con.commit()
        with self.assertRaisesRegex(ValueError, "jobs ativos"):
            migrar_dados.migrar(self.root, self.root.parent / (self.root.name + "_bloqueada"), progresso=lambda _: None)

    def test_inicializacao_nao_substitui_banco(self):
        codigo = "import sys;sys.path.insert(0,sys.argv[1]);from criar_base_inicial import criar;criar(sys.argv[2])"
        destino = self.root / "base_nova"
        cmd = [sys.executable, "-c", codigo, str(ROOT), str(destino)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        digest = hashlib.sha256((destino / "compras.db").read_bytes()).hexdigest()
        result = subprocess.run(cmd, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(digest, hashlib.sha256((destino / "compras.db").read_bytes()).hexdigest())

    def test_manifesto_e_extracao_pacote(self):
        manifest = json.loads((BASE / "release.json").read_text())
        pacotes_radar_pncp.validar_manifesto(manifest)
        path = self.root / "atualizacao.zip"
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
            for name in [*manifest["files"], "release.json"]:
                z.write(BASE / name, name)
        info = pacotes_radar_pncp.inspecionar(path, conferir_hashes=True)
        self.assertEqual(info["version"], "1.0.0")
        out = self.root / "extraido"
        pacotes_radar_pncp.extrair(path, out)
        self.assertTrue((out / "monitoramento.py").is_file())


if __name__ == "__main__":
    unittest.main()
