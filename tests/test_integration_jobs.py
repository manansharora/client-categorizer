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

    idea_id = repo.create_idea(
        "CB KO Hedge",
        "Idea: 3m KO structure for G10 central bank volatility event",
        "tester",
    )
    service.auto_tag_idea(idea_id, "Idea: 3m KO structure for G10 central bank volatility event")

    run_b, results_b = service.match_clients_for_idea(
        idea_text="3m KO structure for G10 central bank risk",
        idea_id=idea_id,
        input_ref="test_job_b",
        top_n=5,
    )
    assert run_b > 0
    assert len(results_b) >= 2
    assert results_b[0]["target_name"] == "Alpha AM"

    run_a, results_a = service.match_ideas_for_client(client_id=c1, input_ref="test_job_a", top_n=5)
    assert run_a > 0
    assert len(results_a) >= 1
    assert results_a[0]["target_name"] == "CB KO Hedge"

    recent = repo.list_recent_match_results(limit=20)
    assert len(recent) >= len(results_a) + len(results_b)

