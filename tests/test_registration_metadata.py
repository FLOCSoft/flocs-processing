import contextlib
from datetime import datetime
import io
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flocs_processing import flocs_processing as cli


class RegistrationMetadataTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = str(Path(temporary.name) / "processing.sqlite")
        self.output = contextlib.redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)
        self.observation = SimpleNamespace(
            startTime=datetime(2020, 4, 3, 12, 30), duration=7200,
            nrStationsCore=24, nrStationsRemote=14, nrStationsInternational=0,
        )
        self.lookup = SimpleNamespace(
            target=self.observation, project="LC12_001", find_observation_by_sasid=Mock(),
        )
        self.stager = Mock(return_value=self.lookup)
        archive = ModuleType("flocs_lta.lta_search")
        archive.ObservationStager = self.stager
        provider = patch.dict(sys.modules, {
            "flocs_lta": ModuleType("flocs_lta"), "flocs_lta.lta_search": archive,
        })
        provider.start()
        self.addCleanup(provider.stop)

    def create(self, pipelines=None, table="processing_flocs"):
        cli.create_database(self.path, table_name=table, pipelines=pipelines or ["linc"])

    def register(self, name="FIELD", target="222", **options):
        cli.add_field(name, ["111"], target, self.path, **options)

    def run_cli(self, arguments):
        try:
            cli.app(arguments)
        except SystemExit as error:
            self.assertEqual(error.code, 0)

    def row(self, target="222", table="processing_flocs"):
        table = '"' + table.replace('"', '""') + '"'
        with sqlite3.connect(self.path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute(f"SELECT * FROM {table} WHERE sas_id_target=?", (target,)).fetchone()
            return dict(row) if row else None

    def test_existing_registration_stays_offline_and_inherits_defaults(self):
        self.create()
        self.register()
        row = self.row()
        self.assertEqual((row["pipelines"], row["status"], row["status_target"], row["downloaded"]),
                         ("linc", "not started", 0, 0))
        self.assertIsNone(row["intent"])
        self.assertIsNotNone(row["last_updated_at"])
        self.stager.assert_not_called()

    def test_cli_registers_archive_metadata_and_widefield_intent(self):
        self.create(["linc", "ddf-pipeline", "vlbi-delay-widefield"])
        self.lookup.project = "O'Brien survey"
        self.run_cli(["add-field", "--field-name", "O'Brien field", "--sas-id-calibrators", "111", "112",
                 "--sas-id-target", "222", "--dbname", self.path,
                 "--intent", "PILOT-widefield", "--fetch-metadata"])
        row = self.row()
        self.assertEqual([row[key] for key in ("project_code", "observation_date", "integration_hours",
                                              "nr_core", "nr_remote", "nr_international")],
                         ["O'Brien survey", "2020-04-03T12:30:00", 2, 24, 14, 0])
        self.assertEqual(row["intent"], "PILOT-widefield")
        self.assertEqual(row["pipelines"], "linc,ddf-pipeline,vlbi-delay-widefield")
        self.assertEqual(row["sas_id_calibrator2"], "112")
        self.assertEqual(row["status_vlbi_delay"], 0)
        self.stager.assert_called_once_with(get_surls=False)
        self.lookup.find_observation_by_sasid.assert_called_once_with("ALL", "222")

    def test_custom_table_and_single_target_intent(self):
        self.create(["linc", "vlbi-delay-single-target"], table="custom_processing")
        self.register(intent="PILOT-single-target", table_name="custom_processing")
        self.assertEqual(self.row(table="custom_processing")["pipelines"], "linc,vlbi-delay-single-target")
        self.stager.assert_not_called()

    def test_explicit_components_are_saved_and_intent_is_inferred(self):
        self.create(["linc", "ddf-pipeline", "vlbi-delay-widefield"])
        self.register(pipelines=" DDF-pipeline, linc, vlbi-delay-widefield ")
        self.assertEqual(self.row()["pipelines"], "ddf-pipeline,linc,vlbi-delay-widefield")
        self.assertEqual(self.row()["intent"], "PILOT-widefield")

    def test_unambiguous_table_default_supplies_intent(self):
        self.create(["linc", "ddf-pipeline", "vlbi-delay-widefield"])
        self.register()
        self.assertEqual(self.row()["intent"], "PILOT-widefield")

    def test_invalid_selections_do_not_insert_or_contact_archive(self):
        self.create(["linc", "ddf-pipeline", "vlbi-delay-widefield"])
        for options in [dict(pipelines="unknown"), dict(intent="PILOT-widefield", pipelines="linc"),
                        dict(intent="PILOT-single-target")]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.register(fetch_metadata=True, **options)
            self.assertIsNone(self.row())
        self.stager.assert_not_called()

    def test_missing_workflow_checkpoints_are_rejected(self):
        self.create()
        with self.assertRaisesRegex(ValueError, "checkpoint columns"):
            self.register(intent="PILOT-widefield")
        self.assertIsNone(self.row())

    def test_one_or_two_calibrators_are_required(self):
        self.create()
        for calibrators in ([], ["1", "2", "3"]):
            with self.subTest(calibrators=calibrators), self.assertRaises(ValueError):
                cli.add_field("FIELD", calibrators, "222", self.path)
        self.assertIsNone(self.row())

    def test_refresh_changes_only_metadata_for_the_selected_field(self):
        self.create(["linc", "ddf-pipeline", "vlbi-delay-widefield"])
        self.register(fetch_metadata=True)
        self.register("OTHER", "333")
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE processing_flocs SET status='processing', status_target=98, "
                       "status_calibrator1=2, status_calibrator2=2, status_vlbi_delay=3, "
                       "downloaded=1, workflow_run_id='active-run', "
                       "last_error='retained detail' WHERE sas_id_target='222'")
        before, other = self.row(), self.row("333")
        self.lookup.project = "LC13_002"
        self.observation.duration = 10800
        self.run_cli(["update-field-metadata", "FIELD", "222", self.path])
        after = self.row()
        changed = {key for key in before if before[key] != after[key]}
        self.assertEqual(changed, {"project_code", "integration_hours", "last_updated_at"})
        self.assertEqual(after["integration_hours"], 3)
        self.assertEqual(self.row("333"), other)

    def test_refresh_keeps_known_values_when_archive_values_are_missing(self):
        self.create()
        self.register(fetch_metadata=True)
        before = self.row()
        self.lookup.project = None
        self.observation.startTime = datetime.min
        self.observation.duration = float("nan")
        self.observation.nrStationsCore = -1
        self.observation.nrStationsRemote = 2.5
        self.observation.nrStationsInternational = None
        cli.update_field_metadata("FIELD", "222", self.path)
        after = self.row()
        self.assertEqual({key: value for key, value in before.items() if key != "last_updated_at"},
                         {key: value for key, value in after.items() if key != "last_updated_at"})

    def test_missing_or_wrong_field_does_not_contact_archive(self):
        self.create()
        self.register()
        for name, target in [("WRONG", "222"), ("FIELD", "999")]:
            with self.subTest(name=name, target=target), self.assertRaises(LookupError):
                cli.update_field_metadata(name, target, self.path)
        self.stager.assert_not_called()

    def test_archive_failures_leave_registration_and_refresh_unchanged(self):
        self.create()
        self.register()
        before = self.row()
        for error in [SystemExit(0), ConnectionError("archive unavailable")]:
            self.lookup.find_observation_by_sasid.side_effect = error
            expected = LookupError if isinstance(error, SystemExit) else ConnectionError
            with self.subTest(error=error), self.assertRaises(expected):
                cli.update_field_metadata("FIELD", "222", self.path)
            self.assertEqual(self.row(), before)
            with self.assertRaises(expected):
                self.register("NEW", "333", fetch_metadata=True)
            self.assertIsNone(self.row("333"))

    def test_duplicate_registration_preserves_progress(self):
        self.create()
        self.register()
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE processing_flocs SET status_target=99, downloaded=1")
        before = self.row()
        with self.assertRaises(sqlite3.IntegrityError):
            self.register(fetch_metadata=True)
        self.assertEqual(self.row(), before)

    def test_legacy_schema_accepts_old_registration_but_rejects_metadata(self):
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TABLE processing_flocs (target_name TEXT, sas_id_target TEXT PRIMARY KEY, "
                       "sas_id_calibrator1 TEXT, sas_id_calibrator2 TEXT, pipelines TEXT DEFAULT 'linc', "
                       "status_calibrator1 INTEGER DEFAULT 0, status_calibrator2 INTEGER DEFAULT 0, "
                       "status_target INTEGER DEFAULT 0)")
        self.register(pipelines="linc")
        self.assertEqual(self.row()["pipelines"], "linc")
        for action in [lambda: self.register("NEW", "333", fetch_metadata=True),
                       lambda: cli.update_field_metadata("FIELD", "222", self.path)]:
            with self.assertRaisesRegex(ValueError, "schema upgrade"):
                action()
        self.stager.assert_not_called()


if __name__ == "__main__":
    unittest.main()
