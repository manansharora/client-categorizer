from __future__ import annotations

import json
from datetime import date

import pandas as pd
import streamlit as st

from core.constants import CLIENT_TYPES, FEEDBACK_LABELS, OBSERVATION_TYPES, REGIONS
from core.database import initialize_database
from core.logging_utils import get_logger
from core.repository import Repository
from core.service import ClientCategorizerService


st.set_page_config(
    page_title="FX Client Categorizer MVP",
    page_icon=":bar_chart:",
    layout="wide",
)


@st.cache_resource
def get_service() -> tuple[Repository, ClientCategorizerService]:
    conn = initialize_database()
    repo = Repository(conn)
    service = ClientCategorizerService(repo)
    return repo, service


repo, service = get_service()
logger = get_logger("app")


def _rows_to_df(rows: list) -> pd.DataFrame:
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def _client_options(clients: list[dict]) -> dict[str, int]:
    return {f'{c["client_name"]} ({c["client_type"]})': int(c["client_id"]) for c in clients}


def page_client_manager() -> None:
    st.header("Client Manager")
    service.refresh_resources()

    st.subheader("Create Client")
    with st.form("create_client_form"):
        client_name = st.text_input("Client Name")
        client_type = st.selectbox("Client Type", CLIENT_TYPES)
        active_flag = st.checkbox("Active", value=True)
        submitted = st.form_submit_button("Create Client")
        if submitted:
            if not client_name.strip():
                st.warning("Client name is required.")
            else:
                try:
                    client_id = repo.create_client(client_name, client_type, int(active_flag))
                    repo.set_manual_tags("CLIENT", client_id, [client_type])
                    st.success(f"Client created: {client_name}")
                except Exception as exc:
                    st.error(f"Could not create client: {exc}")

    clients = [dict(c) for c in repo.list_clients()]
    if not clients:
        st.info("No clients yet.")
        return

    st.subheader("Edit Client")
    client_map = _client_options(clients)
    selected_label = st.selectbox("Select Client", list(client_map.keys()), key="client_edit_selector")
    selected_client_id = client_map[selected_label]
    selected_client = next(c for c in clients if int(c["client_id"]) == selected_client_id)

    with st.form("edit_client_form"):
        edit_type = st.selectbox(
            "Client Type",
            CLIENT_TYPES,
            index=CLIENT_TYPES.index(selected_client["client_type"])
            if selected_client["client_type"] in CLIENT_TYPES
            else 0,
        )
        edit_active = st.checkbox("Active", value=bool(selected_client["active_flag"]))
        edit_submit = st.form_submit_button("Update Client")
        if edit_submit:
            repo.update_client(selected_client_id, edit_type, int(edit_active))
            st.success("Client updated.")

    st.subheader("Add Observation")
    with st.form("add_obs_form"):
        obs_type = st.selectbox("Observation Type", OBSERVATION_TYPES)
        obs_text = st.text_area("Observation Text", height=120)
        obs_date = st.date_input("Observation Date", value=date.today())
        source_confidence = st.slider("Source Confidence", 0.0, 1.0, 0.8, 0.05)
        obs_submit = st.form_submit_button("Add Observation")
        if obs_submit:
            if not obs_text.strip():
                st.warning("Observation text is required.")
            else:
                obs_id = repo.add_observation(
                    client_id=selected_client_id,
                    obs_type=obs_type,
                    obs_text=obs_text,
                    obs_date=obs_date,
                    source_confidence=source_confidence,
                )
                service.auto_tag_observation(obs_id, obs_text)
                service.refresh_client_tags_from_observations(selected_client_id)
                st.success("Observation added and auto-tagged.")

    st.subheader("Manual Client Tag Overrides")
    all_tags = [dict(t) for t in repo.list_taxonomy_tags()]
    existing_manual = [
        t["tag_code"] for t in repo.list_entity_tags("CLIENT", selected_client_id) if t["origin"] == "MANUAL"
    ]
    with st.form("manual_client_tags"):
        selected_codes = st.multiselect(
            "Manual Tags",
            options=[t["tag_code"] for t in all_tags],
            default=existing_manual,
        )
        tag_submit = st.form_submit_button("Save Manual Tags")
        if tag_submit:
            repo.set_manual_tags("CLIENT", selected_client_id, selected_codes)
            st.success("Manual tags saved.")

    st.subheader("Client Observations")
    obs_df = _rows_to_df(repo.list_observations(selected_client_id))
    if obs_df.empty:
        st.info("No observations for this client.")
    else:
        st.dataframe(obs_df, width="stretch")


def page_idea_manager() -> None:
    st.header("Idea Manager")
    service.refresh_resources()

    st.subheader("Create Idea/Event")
    with st.form("create_idea_form"):
        idea_title = st.text_input("Idea Title")
        idea_text = st.text_area("Idea Text", height=140)
        created_by = st.text_input("Created By (optional)")
        submitted = st.form_submit_button("Create Idea")
        if submitted:
            if not idea_title.strip() or not idea_text.strip():
                st.warning("Idea title and text are required.")
            else:
                idea_id = repo.create_idea(idea_title, idea_text, created_by)
                service.auto_tag_idea(idea_id, idea_text)
                st.success("Idea saved and auto-tagged.")

    ideas = [dict(i) for i in repo.list_ideas()]
    if not ideas:
        st.info("No ideas yet.")
        return

    st.subheader("Edit Idea")
    idea_options = {f'{i["idea_title"]} (#{i["idea_id"]})': int(i["idea_id"]) for i in ideas}
    selected = st.selectbox("Select Idea", list(idea_options.keys()), key="idea_edit_selector")
    selected_idea_id = idea_options[selected]
    selected_idea = next(i for i in ideas if int(i["idea_id"]) == selected_idea_id)

    with st.form("edit_idea_form"):
        edit_title = st.text_input("Title", value=selected_idea["idea_title"])
        edit_text = st.text_area("Text", value=selected_idea["idea_text"], height=120)
        edit_by = st.text_input("Created By", value=selected_idea.get("created_by") or "")
        edit_submit = st.form_submit_button("Update Idea")
        if edit_submit:
            repo.update_idea(selected_idea_id, edit_title, edit_text, edit_by)
            service.auto_tag_idea(selected_idea_id, edit_text)
            st.success("Idea updated.")

    all_tags = [dict(t) for t in repo.list_taxonomy_tags()]
    existing_manual = [
        t["tag_code"] for t in repo.list_entity_tags("IDEA", selected_idea_id) if t["origin"] == "MANUAL"
    ]
    with st.form("manual_idea_tags"):
        selected_codes = st.multiselect(
            "Manual Idea Tags",
            options=[t["tag_code"] for t in all_tags],
            default=existing_manual,
        )
        tag_submit = st.form_submit_button("Save Manual Tags")
        if tag_submit:
            repo.set_manual_tags("IDEA", selected_idea_id, selected_codes)
            st.success("Manual tags saved.")

    st.subheader("Idea List")
    st.dataframe(_rows_to_df(repo.list_ideas()), width="stretch")


def _render_feedback_controls(run_id: int, results: list[dict[str, object]], target_label: str) -> None:
    st.markdown("### Feedback")
    for row in results[:10]:
        target_id = int(row["target_entity_id"])
        key_prefix = f"{run_id}_{target_label}_{target_id}"
        with st.expander(f'{row["target_name"]}  |  Score: {row["final_score"]:.3f}'):
            explanation = row["explanation"]
            st.write(explanation["explanation_text"])
            st.write("Matched tags:", ", ".join(explanation["matched_tags"]) or "None")
            st.write("Top terms:", ", ".join(explanation["top_terms"]) or "None")
            feature_evidence = explanation.get("feature_evidence")
            if feature_evidence:
                st.write("Feature evidence:", feature_evidence)
            pm_drilldown = row.get("pm_drilldown", [])
            if pm_drilldown:
                st.write("PM drilldown:")
                pm_rows = []
                for pm in pm_drilldown:
                    pm_rows.append(
                        {
                            "pm_name": pm.get("pm_name", ""),
                            "pm_score": pm.get("pm_score", 0.0),
                            "semantic_score": pm.get("semantic_score", 0.0),
                            "lexical_score": pm.get("lexical_score", 0.0),
                            "structured_score": pm.get("structured_score", 0.0),
                            "top_terms": ", ".join(pm.get("top_terms", [])),
                            "explanation": pm.get("explanation", ""),
                        }
                    )
                st.dataframe(pd.DataFrame(pm_rows), width="stretch")
            fb_label = st.selectbox("Feedback", FEEDBACK_LABELS, key=f"fb_label_{key_prefix}")
            fb_comment = st.text_input("Comment (optional)", key=f"fb_comment_{key_prefix}")
            if st.button("Save Feedback", key=f"fb_save_{key_prefix}"):
                repo.add_feedback(run_id, target_id, fb_label, fb_comment.strip() or None)
                st.success("Feedback saved.")


def page_match_clients_for_idea() -> None:
    st.header("Match Clients for Idea (Job B)")
    service.refresh_resources()

    ideas = [dict(i) for i in repo.list_ideas()]
    use_stored = st.checkbox("Use stored idea", value=True)

    idea_id = None
    idea_text = ""
    input_ref = "adhoc"
    target_region = st.selectbox("Target Region", REGIONS, index=0)
    target_country = st.text_input("Target Country (optional)", value="")
    if use_stored and ideas:
        options = {f'{i["idea_title"]} (#{i["idea_id"]})': int(i["idea_id"]) for i in ideas}
        selected = st.selectbox("Choose Idea", list(options.keys()))
        idea_id = options[selected]
        idea = next(i for i in ideas if int(i["idea_id"]) == idea_id)
        idea_text = idea["idea_text"]
        input_ref = f"idea:{idea_id}"
        st.text_area("Idea Text Preview", value=idea_text, height=120, disabled=True)
    else:
        idea_text = st.text_area("Idea/Event Text", height=160)
        input_ref = "idea:adhoc"

    if st.button("Run Client Matching", type="primary"):
        if not idea_text.strip():
            st.warning("Idea text is required.")
        else:
            run_id, results, meta = service.match_clients_for_idea(
                idea_text=idea_text,
                idea_id=idea_id,
                input_ref=input_ref,
                top_n=25,
                region=target_region,
                country=target_country.strip() or None,
            )
            st.session_state["last_job_b"] = {"run_id": run_id, "results": results, "meta": meta}
            logger.info(
                "ui_match_clients run_id=%s results=%s region=%s country=%s",
                run_id,
                len(results),
                target_region,
                target_country.strip() or "",
            )
            st.success(f"Matching complete. Run ID: {run_id}")

    payload = st.session_state.get("last_job_b")
    if payload:
        results = payload["results"]
        run_id = int(payload["run_id"])
        meta = payload.get("meta")
        if meta:
            st.info(
                f"Region target: {meta.get('target_region')} | Country: {meta.get('target_country') or 'N/A'} | "
                f"Fallback used: {meta.get('fallback_used')}"
            )
            if meta.get("empty_reason") and not results:
                st.warning(meta.get("empty_reason"))
            with st.expander("Debug details"):
                st.json(meta.get("debug", []))
        table_rows = [
            {
                "target_name": r["target_name"],
                "final_score": r["final_score"],
                "structured_score": r.get("structured_score", 0.0),
                "semantic_score": r["semantic_score"],
                "lexical_score": r["lexical_score"],
                "taxonomy_score": r["taxonomy_score"],
                "feature_region": (r["explanation"].get("feature_evidence") or {}).get("region", ""),
                "feature_stage": (r["explanation"].get("feature_evidence") or {}).get("stage", ""),
                "matched_tags": ", ".join(r["explanation"]["matched_tags"]),
                "top_terms": ", ".join(r["explanation"]["top_terms"]),
                "explanation_text": r["explanation"]["explanation_text"],
            }
            for r in results
        ]
        st.dataframe(pd.DataFrame(table_rows), width="stretch")
        _render_feedback_controls(run_id, results, "client")


def page_match_ideas_for_client() -> None:
    st.header("Match Ideas for Client (Job A)")
    service.refresh_resources()
    clients = [dict(c) for c in repo.list_clients(active_only=True)]
    if not clients:
        st.info("No active clients available.")
        return

    options = _client_options(clients)
    selected = st.selectbox("Choose Client", list(options.keys()))
    client_id = options[selected]

    with st.expander("Optional: Add a fresh observation before matching"):
        with st.form("quick_obs_form"):
            obs_type = st.selectbox("Observation Type", OBSERVATION_TYPES, key="quick_obs_type")
            obs_text = st.text_area("Observation Text", key="quick_obs_text")
            obs_conf = st.slider("Source Confidence", 0.0, 1.0, 0.8, 0.05, key="quick_obs_conf")
            add_obs = st.form_submit_button("Add Observation")
            if add_obs:
                if obs_text.strip():
                    obs_id = repo.add_observation(client_id, obs_type, obs_text, date.today(), obs_conf)
                    service.auto_tag_observation(obs_id, obs_text)
                    service.refresh_client_tags_from_observations(client_id)
                    st.success("Observation added and tags refreshed.")
                else:
                    st.warning("Observation text is empty.")

    if st.button("Run Idea Matching", type="primary"):
        run_id, results = service.match_ideas_for_client(client_id=client_id, input_ref=f"client:{client_id}", top_n=25)
        st.session_state["last_job_a"] = {"run_id": run_id, "results": results}
        st.success(f"Matching complete. Run ID: {run_id}")

    payload = st.session_state.get("last_job_a")
    if payload:
        results = payload["results"]
        run_id = int(payload["run_id"])
        table_rows = [
            {
                "target_name": r["target_name"],
                "final_score": r["final_score"],
                "semantic_score": r["semantic_score"],
                "lexical_score": r["lexical_score"],
                "taxonomy_score": r["taxonomy_score"],
                "matched_tags": ", ".join(r["explanation"]["matched_tags"]),
                "top_terms": ", ".join(r["explanation"]["top_terms"]),
                "explanation_text": r["explanation"]["explanation_text"],
            }
            for r in results
        ]
        st.dataframe(pd.DataFrame(table_rows), width="stretch")
        _render_feedback_controls(run_id, results, "idea")


def page_taxonomy_synonyms() -> None:
    st.header("Taxonomy & Synonyms")
    service.refresh_resources()

    st.subheader("Current Taxonomy")
    st.dataframe(_rows_to_df(repo.list_taxonomy_tags()), width="stretch")

    with st.form("add_tag_form"):
        st.markdown("#### Add Taxonomy Tag")
        tag_family = st.text_input("Tag Family (e.g. PRODUCT)")
        tag_code = st.text_input("Tag Code (e.g. FX_VANILLA_OPTION)")
        tag_label = st.text_input("Tag Label")
        tag_version = st.text_input("Taxonomy Version", value="v1")
        submit_tag = st.form_submit_button("Add Tag")
        if submit_tag:
            if tag_family and tag_code and tag_label:
                repo.add_taxonomy_tag(tag_family, tag_code, tag_label, tag_version)
                service.refresh_resources()
                st.success("Tag added.")
            else:
                st.warning("Tag family, code, and label are required.")

    st.subheader("Current Synonyms")
    st.dataframe(_rows_to_df(repo.list_synonyms()), width="stretch")

    with st.form("add_synonym_form"):
        st.markdown("#### Add Synonym")
        surface = st.text_input("Surface Form")
        canonical = st.text_input("Canonical Form")
        tags = [dict(t)["tag_code"] for t in repo.list_taxonomy_tags()]
        optional_tag = st.selectbox("Optional Tag Code", [""] + tags)
        syn_version = st.text_input("Taxonomy Version", value="v1", key="syn_version")
        submit_syn = st.form_submit_button("Add Synonym")
        if submit_syn:
            if surface and canonical:
                repo.add_synonym(surface, canonical, optional_tag or None, syn_version)
                service.refresh_resources()
                st.success("Synonym added.")
            else:
                st.warning("Surface and canonical forms are required.")


def page_feedback_review() -> None:
    st.header("Feedback Review")
    fb_df = _rows_to_df(repo.list_feedback(limit=500))
    if fb_df.empty:
        st.info("No feedback records yet.")
    else:
        st.dataframe(fb_df, width="stretch")

    st.subheader("Recent Match Results")
    rows = repo.list_recent_match_results(limit=200)
    if not rows:
        st.info("No match results yet.")
        return
    records = []
    for row in rows:
        record = dict(row)
        explanation = json.loads(record["explanation_json"])
        record["matched_tags"] = ", ".join(explanation.get("matched_tags", []))
        record["top_terms"] = ", ".join(explanation.get("top_terms", []))
        record["explanation_text"] = explanation.get("explanation_text", "")
        records.append(record)
    st.dataframe(pd.DataFrame(records), width="stretch")


st.title("FX Structuring Client-Idea Relevance Engine")
page = st.sidebar.radio(
    "Navigation",
    [
        "Client Manager",
        "Idea Manager",
        "Match Clients for Idea",
        "Match Ideas for Client",
        "Taxonomy & Synonyms",
        "Feedback Review",
    ],
)

if page == "Client Manager":
    page_client_manager()
elif page == "Idea Manager":
    page_idea_manager()
elif page == "Match Clients for Idea":
    page_match_clients_for_idea()
elif page == "Match Ideas for Client":
    page_match_ideas_for_client()
elif page == "Taxonomy & Synonyms":
    page_taxonomy_synonyms()
else:
    page_feedback_review()
