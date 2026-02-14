from datetime import date

from core.database import initialize_database
from core.repository import Repository
from core.service import ClientCategorizerService


def test_job_b_and_job_a_end_to_end(tmp_path) -> None:
    db_path = tmp_path / "integration.db"
    conn = initialize_database(db_path)
    repo = Repository(conn)
    service = ClientCategorizerService(repo)

    c1 = repo.create_client("Alpha AM", "ASSET_MANAGER_MULTI_ASSET", 1)
    c2 = repo.create_client("Beta HF", "HF_MACRO", 1)

    obs1 = repo.add_observation(
        client_id=c1,
        obs_type="TRADE_NOTE",
        obs_text="Client discussed 3m KO hedge in G10 and central bank event risk",
        obs_date=date.today(),
        source_confidence=0.95,
    )
    obs2 = repo.add_observation(
        client_id=c2,
        obs_type="CALL_NOTE",
        obs_text="Client likes directional carry in EM LATAM",
        obs_date=date.today(),
        source_confidence=0.9,
    )

    service.auto_tag_observation(obs1, "Client discussed 3m KO hedge in G10 and central bank event risk")
    service.auto_tag_observation(obs2, "Client likes directional carry in EM LATAM")
    service.refresh_client_tags_from_observations(c1)
    service.refresh_client_tags_from_observations(c2)
    repo.upsert_entity_profile_cache("CLIENT", c1, "EURUSD KO central bank hedge")
    repo.upsert_entity_profile_cache("CLIENT", c2, "EMLATAM carry directional")
    repo.upsert_rfq_features_bulk(
        [
            (
                "CLIENT",
                c1,
                "EUROPE",
                "UNITED KINGDOM",
                "PAIR_PRODUCT",
                "EURUSD",
                "KO",
                None,
                12,
                100.0,
                date.today().isoformat(),
                5.0,
                8.0,
                12.0,
                7.0,
            ),
            (
                "CLIENT",
                c2,
                "EUROPE",
                "UNITED KINGDOM",
                "PAIR_PRODUCT",
                "USDBRL",
                "KNO",
                None,
                8,
                80.0,
                date.today().isoformat(),
                2.0,
                4.0,
                8.0,
                3.0,
            ),
        ]
    )
    pm_id = repo.upsert_pm(c1, "PM Alpha", 1)
    repo.add_pm_observation(
        pm_id=pm_id,
        obs_type="PREFERENCE_NOTE",
        obs_text="Macro directional, likes EURUSD KO and central bank themes",
        obs_date=date.today(),
        source_confidence=0.9,
    )
    repo.upsert_entity_profile_cache("PM", pm_id, "Macro directional | EURUSD KO | central bank")
    repo.upsert_rfq_features_bulk(
        [
            (
                "PM",
                pm_id,
                "EUROPE",
                "UNITED KINGDOM",
                "PAIR_PRODUCT",
                "EURUSD",
                "KO",
                None,
                6,
                30.0,
                date.today().isoformat(),
                2.0,
                3.0,
                5.0,
                3.5,
            )
        ]
    )

    idea_id = repo.create_idea(
        "CB KO Hedge",
        "Idea: EURUSD KO structure for Europe central bank volatility event",
        "tester",
    )
    service.auto_tag_idea(idea_id, "Idea: EURUSD KO structure for Europe central bank volatility event")

    run_b, results_b, meta_b = service.match_clients_for_idea(
        idea_text="EURUSD KO structure for Europe central bank risk",
        idea_id=idea_id,
        input_ref="test_job_b",
        top_n=5,
        region="EUROPE",
    )
    assert run_b > 0
    assert meta_b["target_region"] == "EUROPE"
    assert len(results_b) >= 1
    assert results_b[0]["target_name"] == "Alpha AM"
    assert "pm_drilldown" in results_b[0]
    assert len(results_b[0]["pm_drilldown"]) >= 1
    assert "explanation" in results_b[0]["pm_drilldown"][0]
    assert "top_terms" in results_b[0]["pm_drilldown"][0]

    run_a, results_a = service.match_ideas_for_client(client_id=c1, input_ref="test_job_a", top_n=5)
    assert run_a > 0
    assert len(results_a) >= 1
    assert results_a[0]["target_name"] == "CB KO Hedge"

    recent = repo.list_recent_match_results(limit=20)
    assert len(recent) >= len(results_a) + len(results_b)


def test_job_b_candidate_stage_filters_do_not_overconstrain(tmp_path) -> None:
    db_path = tmp_path / "integration_stage_filters.db"
    conn = initialize_database(db_path)
    repo = Repository(conn)
    service = ClientCategorizerService(repo)

    c1 = repo.create_client("Citadel Demo", "HF_MACRO", 1)
    c2 = repo.create_client("Brevan Demo", "HF_MACRO", 1)
    repo.upsert_entity_profile_cache("CLIENT", c1, "USDJPY risk reversal short tenor")
    repo.upsert_entity_profile_cache("CLIENT", c2, "Dual digital directional short vol")

    today = date.today().isoformat()
    repo.upsert_rfq_features_bulk(
        [
            (
                "CLIENT",
                c1,
                "AMERICA",
                "UNITED STATES",
                "PAIR",
                "USDJPY",
                None,
                None,
                4,
                20.0,
                today,
                1.5,
                2.5,
                4.0,
                2.0,
            ),
            (
                "CLIENT",
                c1,
                "AMERICA",
                "UNITED STATES",
                "PAIR_PRODUCT",
                "USDJPY",
                "RKO",
                None,
                2,
                10.0,
                today,
                1.0,
                1.5,
                2.0,
                1.2,
            ),
            (
                "CLIENT",
                c2,
                "EUROPE",
                "UNITED KINGDOM",
                "PRODUCT",
                None,
                "DIGKNO",
                None,
                3,
                15.0,
                today,
                1.2,
                2.0,
                3.1,
                1.8,
            ),
        ]
    )

    _, results_usdjpy, meta_usdjpy = service.match_clients_for_idea(
        idea_text="USDJPY risk reversal, short tenor 1W",
        input_ref="test_stage_usdjpy",
        top_n=5,
        region="AMERICA",
    )
    assert len(results_usdjpy) >= 1
    assert results_usdjpy[0]["target_name"] == "Citadel Demo"
    assert any(d["row_count"] > 0 for d in meta_usdjpy["debug"])

    _, results_digital, meta_digital = service.match_clients_for_idea(
        idea_text="AUDUSD/AUDJPY dual digital directional short vol",
        input_ref="test_stage_digital",
        top_n=5,
        region="EUROPE",
    )
    assert len(results_digital) >= 1
    assert results_digital[0]["target_name"] == "Brevan Demo"
    assert any(d["row_count"] > 0 for d in meta_digital["debug"])


def test_job_b_structured_signals_use_raw_idea_text(tmp_path) -> None:
    db_path = tmp_path / "integration_raw_signal.db"
    conn = initialize_database(db_path)
    repo = Repository(conn)
    service = ClientCategorizerService(repo)

    c1 = repo.create_client("Risk Reversal Demo", "HF_MACRO", 1)
    repo.upsert_entity_profile_cache("CLIENT", c1, "USDJPY risk reversal short tenor")
    today = date.today().isoformat()
    repo.upsert_rfq_features_bulk(
        [
            (
                "CLIENT",
                c1,
                "AMERICA",
                "UNITED STATES",
                "PAIR_PRODUCT",
                "USDJPY",
                "RKO",
                None,
                3,
                20.0,
                today,
                1.0,
                1.5,
                2.0,
                1.4,
            )
        ]
    )

    _, results, meta = service.match_clients_for_idea(
        idea_text="USD/JPY risk reversal 1W",
        input_ref="test_raw_signal",
        top_n=5,
        region="AMERICA",
    )
    assert len(results) >= 1
    assert results[0]["target_name"] == "Risk Reversal Demo"
    assert str(meta["signals"]["ccy_pair"]) == "USDJPY"
    assert str(meta["signals"]["product_type"]) == "RKO"


def test_job_b_returns_global_pm_matches_outside_top_clients(tmp_path) -> None:
    db_path = tmp_path / "integration_global_pm.db"
    conn = initialize_database(db_path)
    repo = Repository(conn)
    service = ClientCategorizerService(repo)

    c1 = repo.create_client("Client Top", "HF_MACRO", 1)
    c2 = repo.create_client("Client PMOnly", "HF_MACRO", 1)
    repo.upsert_entity_profile_cache("CLIENT", c1, "USDJPY risk reversal")
    repo.upsert_entity_profile_cache("CLIENT", c2, "Unrelated profile text")

    today = date.today().isoformat()
    repo.upsert_rfq_features_bulk(
        [
            (
                "CLIENT",
                c1,
                "AMERICA",
                "UNITED STATES",
                "PAIR_PRODUCT",
                "USDJPY",
                "RKO",
                None,
                8,
                50.0,
                today,
                4.0,
                6.0,
                8.0,
                5.0,
            ),
            (
                "CLIENT",
                c2,
                "AMERICA",
                "UNITED STATES",
                "PAIR_PRODUCT",
                "EURUSD",
                "DIG",
                None,
                2,
                10.0,
                today,
                0.5,
                0.8,
                1.1,
                0.6,
            ),
        ]
    )

    pm_id = repo.upsert_pm(c2, "PM Global", 1)
    repo.upsert_entity_profile_cache("PM", pm_id, "USDJPY risk reversal short tenor")
    repo.upsert_rfq_features_bulk(
        [
            (
                "PM",
                pm_id,
                "AMERICA",
                "UNITED STATES",
                "PAIR_PRODUCT",
                "USDJPY",
                "RKO",
                None,
                6,
                30.0,
                today,
                2.5,
                4.0,
                5.5,
                3.5,
            )
        ]
    )

    _, results, meta = service.match_clients_for_idea(
        idea_text="USD/JPY risk reversal 1W",
        input_ref="test_global_pm",
        top_n=1,
        region="AMERICA",
    )
    assert len(results) == 1
    assert results[0]["target_name"] == "Client Top"
    assert "pm_global_results" in meta
    assert any(row["pm_name"] == "PM Global" for row in meta["pm_global_results"])
