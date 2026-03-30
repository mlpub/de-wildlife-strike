import os

import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery

st.set_page_config(
    page_title="Wildlife Strike Dashboard",
    page_icon=":bird:",
    layout="wide"
)

st.title("Wildlife Strike Analytics Dashboard")
st.caption("BigQuery-powered Streamlit dashboard")

PROJECT_ID = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
MART_DATASET = os.getenv("BQ_MART_DATASET", "dev_mart")

if not PROJECT_ID:
    st.error("Missing GCP_PROJECT_ID or GOOGLE_CLOUD_PROJECT")
    st.stop()


def t(table_name: str) -> str:
    return f"`{PROJECT_ID}.{MART_DATASET}.{table_name}`"


@st.cache_resource
def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def run_query(query: str, params: list[bigquery.ScalarQueryParameter] | None = None) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    return get_client().query(query, job_config=job_config).to_dataframe()


@st.cache_data(ttl=300)
def load_kpis(start_date=None, end_date=None, airport_name=None):
    query = f"""
        SELECT
            COUNT(*) AS total_incidents,
            COALESCE(SUM(CAST(aircraft_damage AS INT64)), 0) AS total_aircraft_damage,
            COALESCE(SUM(CAST(injuries AS INT64)), 0) AS total_injuries
        FROM {t('fact_wildlife_strike')} f
        JOIN {t('dim_date')} d ON f.date_id = d.id
        LEFT JOIN {t('dim_airport')} a ON f.airport_id = a.id
        WHERE (@start_date IS NULL OR d.full_date >= @start_date)
          AND (@end_date IS NULL OR d.full_date <= @end_date)
          AND (@airport_name IS NULL OR a.airport_name = @airport_name)
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        bigquery.ScalarQueryParameter("airport_name", "STRING", airport_name),
    ]
    return run_query(query, params)


@st.cache_data(ttl=300)
def load_yearly_trend(start_date=None, end_date=None, airport_name=None):
    query = f"""
        SELECT
            d.year,
            COUNT(*) AS total_incidents
        FROM {t('fact_wildlife_strike')} f
        JOIN {t('dim_date')} d ON f.date_id = d.id
        LEFT JOIN {t('dim_airport')} a ON f.airport_id = a.id
        WHERE (@start_date IS NULL OR d.full_date >= @start_date)
          AND (@end_date IS NULL OR d.full_date <= @end_date)
          AND (@airport_name IS NULL OR a.airport_name = @airport_name)
        GROUP BY d.year
        ORDER BY d.year
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        bigquery.ScalarQueryParameter("airport_name", "STRING", airport_name),
    ]
    return run_query(query, params)


@st.cache_data(ttl=300)
def load_top_airports(limit=10):
    query = f"""
        SELECT
            a.airport_name,
            a.state,
            COUNT(*) AS total_incidents
        FROM {t('fact_wildlife_strike')} f
        JOIN {t('dim_airport')} a ON f.airport_id = a.id
        GROUP BY a.airport_name, a.state
        ORDER BY total_incidents DESC
        LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    return run_query(query, params)


@st.cache_data(ttl=300)
def load_top_species(start_date=None, end_date=None, airport_name=None, limit=10):
    query = f"""
        SELECT
            s.species_name,
            COUNT(*) AS total_incidents
        FROM {t('fact_wildlife_strike')} f
        JOIN {t('dim_date')} d ON f.date_id = d.id
        LEFT JOIN {t('dim_airport')} a ON f.airport_id = a.id
        JOIN {t('dim_species')} s ON f.species_id = s.id
        WHERE (@start_date IS NULL OR d.full_date >= @start_date)
          AND (@end_date IS NULL OR d.full_date <= @end_date)
          AND (@airport_name IS NULL OR a.airport_name = @airport_name)
        GROUP BY s.species_name
        ORDER BY total_incidents DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        bigquery.ScalarQueryParameter("airport_name", "STRING", airport_name),
        bigquery.ScalarQueryParameter("limit", "INT64", int(limit)),
    ]
    return run_query(query, params)


@st.cache_data(ttl=300)
def load_phase_distribution(start_date=None, end_date=None, airport_name=None):
    query = f"""
        SELECT
            p.flight_phase,
            COUNT(*) AS total_incidents
        FROM {t('fact_wildlife_strike')} f
        JOIN {t('dim_date')} d ON f.date_id = d.id
        LEFT JOIN {t('dim_airport')} a ON f.airport_id = a.id
        JOIN {t('dim_flight_phase')} p ON f.flight_phase_id = p.id
        WHERE (@start_date IS NULL OR d.full_date >= @start_date)
          AND (@end_date IS NULL OR d.full_date <= @end_date)
          AND (@airport_name IS NULL OR a.airport_name = @airport_name)
        GROUP BY p.flight_phase
        ORDER BY total_incidents DESC
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        bigquery.ScalarQueryParameter("airport_name", "STRING", airport_name),
    ]
    return run_query(query, params)


@st.cache_data(ttl=300)
def load_detail_data(start_date=None, end_date=None, airport_name=None, limit=1000):
    query = f"""
        SELECT
            d.full_date,
            a.airport_name,
            a.state,
            o.operator_name,
            ac.aircraft,
            s.species_name,
            p.flight_phase,
            f.injuries,
            f.aircraft_damage
        FROM {t('fact_wildlife_strike')} f
        JOIN {t('dim_date')} d ON f.date_id = d.id
        LEFT JOIN {t('dim_airport')} a ON f.airport_id = a.id
        LEFT JOIN {t('dim_operator')} o ON f.operator_id = o.id
        LEFT JOIN {t('dim_aircraft')} ac ON f.aircraft_id = ac.id
        LEFT JOIN {t('dim_species')} s ON f.species_id = s.id
        LEFT JOIN {t('dim_flight_phase')} p ON f.flight_phase_id = p.id
        WHERE (@start_date IS NULL OR d.full_date >= @start_date)
          AND (@end_date IS NULL OR d.full_date <= @end_date)
          AND (@airport_name IS NULL OR a.airport_name = @airport_name)
        ORDER BY d.full_date DESC
        LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        bigquery.ScalarQueryParameter("airport_name", "STRING", airport_name),
        bigquery.ScalarQueryParameter("limit", "INT64", int(limit)),
    ]
    return run_query(query, params)


@st.cache_data
def load_airport_options():
    query = f"""
        SELECT DISTINCT airport_name
        FROM {t('dim_airport')}
        WHERE airport_name IS NOT NULL
          AND airport_name <> 'UNKNOWN'
        ORDER BY airport_name
    """
    df = run_query(query)
    return df["airport_name"].tolist()


st.sidebar.header("Filters")

airport_options = load_airport_options()
selected_airport = st.sidebar.selectbox(
    "Airport",
    options=[None] + airport_options,
    format_func=lambda x: "All Airports" if x is None else x
)

start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2015-01-01"))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2015-09-30"))

kpi_df = load_kpis(
    start_date=start_date,
    end_date=end_date,
    airport_name=selected_airport
)
kpi = kpi_df.iloc[0]

trend_df = load_yearly_trend(
    start_date=start_date,
    end_date=end_date,
    airport_name=selected_airport
)
airport_df = load_top_airports()
species_df = load_top_species(
    start_date=start_date,
    end_date=end_date,
    airport_name=selected_airport
)
phase_df = load_phase_distribution(
    start_date=start_date,
    end_date=end_date,
    airport_name=selected_airport
)

col1, col2, col3 = st.columns(3)
col1.metric("Total Incidents", f"{int(kpi['total_incidents']):,}")
col2.metric("Total Aircraft Damage", f"{int(kpi['total_aircraft_damage']):,}")
col3.metric("People Injured", f"{int(kpi['total_injuries']):,}")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Incidents by Year")
    fig_trend = px.line(
        trend_df,
        x="year",
        y="total_incidents",
        markers=True,
        labels={
            "total_incidents": "Total Incidents",
            "year": "Year"
        }
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with right:
    st.subheader("Top 10 Airports by Incidents")
    fig_airport = px.bar(
        airport_df,
        x="total_incidents",
        y="airport_name",
        orientation="h",
        hover_data=["state"],
        labels={
            "total_incidents": "Total Incidents",
            "airport_name": "Airport"
        }
    )
    fig_airport.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_airport, use_container_width=True)

left2, right2 = st.columns(2)

with left2:
    st.subheader("Top 10 Species Involved")
    fig_species = px.bar(
        species_df,
        x="total_incidents",
        y="species_name",
        orientation="h",
        labels={
            "total_incidents": "Total Incidents",
            "species_name": "Species"
        }
    )
    fig_species.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_species, use_container_width=True)

with right2:
    st.subheader("Flight Phase Distribution")
    fig_phase = px.pie(
        phase_df,
        names="flight_phase",
        values="total_incidents"
    )
    st.plotly_chart(fig_phase, use_container_width=True)

st.divider()

st.subheader("Recent Incident Records")
detail_df = load_detail_data(
    start_date=start_date,
    end_date=end_date,
    airport_name=selected_airport,
    limit=1000
)

detail_df = detail_df.rename(columns={
    "full_date": "Date",
    "airport_name": "Airport Name",
    "state": "State",
    "operator_name": "Operator",
    "aircraft": "Aircraft",
    "species_name": "Species",
    "flight_phase": "Flight Phase",
    "injuries": "Injuries",
    "aircraft_damage": "Aircraft Damage"
})

st.dataframe(detail_df, use_container_width=True)
