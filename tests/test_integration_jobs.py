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
