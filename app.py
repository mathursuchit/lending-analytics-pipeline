"""
Lending Analytics Dashboard
Reads directly from a pre-built DuckDB database (built locally via dbt).
"""
import os
import duckdb
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       'lending_analytics', 'lending_analytics.duckdb')


@st.cache_data
def load_data():
    con = duckdb.connect(DB_PATH, read_only=True)
    grade_perf = con.execute("SELECT * FROM main_marts.loan_performance_by_grade ORDER BY risk_grade").df()
    segments   = con.execute("SELECT * FROM main_marts.borrower_risk_segments ORDER BY default_rate_pct DESC").df()
    vintage    = con.execute("SELECT * FROM main_marts.vintage_analysis ORDER BY issue_month, risk_grade").df()
    con.close()
    return grade_perf, segments, vintage


grade_perf, segments, vintage = load_data()

st.set_page_config(page_title='Lending Analytics Pipeline', page_icon='🏦', layout='wide')

st.title('🏦 Lending Analytics Pipeline')
st.caption('dbt + DuckDB | Staging → Intermediate → Marts | 5,000 loan records')

# --- KPIs ---
total_loans  = int(grade_perf['total_loans'].sum())
total_volume = grade_perf['total_loan_volume'].sum()
avg_default  = grade_perf['defaulted_loans'].sum() / total_loans * 100
avg_rate     = (grade_perf['avg_interest_rate_pct'] * grade_perf['total_loans']).sum() / total_loans

col1, col2, col3, col4 = st.columns(4)
col1.metric('Total Loans',            f'{total_loans:,}')
col2.metric('Total Volume',           f'${total_volume/1e6:.1f}M')
col3.metric('Portfolio Default Rate', f'{avg_default:.1f}%')
col4.metric('Avg Interest Rate',      f'{avg_rate:.1f}%')

st.divider()

# --- Default rate & interest rate by grade ---
st.subheader('Default Rate & Interest Rate by Risk Grade')
col_a, col_b = st.columns(2)

with col_a:
    fig = px.bar(
        grade_perf, x='risk_grade', y='default_rate_pct',
        color='default_rate_pct', color_continuous_scale='Reds',
        labels={'risk_grade': 'Risk Grade', 'default_rate_pct': 'Default Rate (%)'},
        title='Default Rate by Grade'
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=grade_perf['risk_grade'], y=grade_perf['avg_interest_rate_pct'],
        name='Avg Interest Rate (%)', marker_color='steelblue'
    ))
    fig2.add_trace(go.Scatter(
        x=grade_perf['risk_grade'], y=grade_perf['avg_fico_score'],
        name='Avg FICO Score', yaxis='y2',
        mode='lines+markers', line=dict(color='orange', width=2), marker=dict(size=8)
    ))
    fig2.update_layout(
        title='Interest Rate vs Avg FICO by Grade',
        yaxis=dict(title='Avg Interest Rate (%)'),
        yaxis2=dict(title='Avg FICO Score', overlaying='y', side='right'),
        legend=dict(orientation='h', y=1.1)
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# --- Borrower risk segments ---
st.subheader('Borrower Risk Segments — Default Rate by FICO & DTI')
fig3 = px.scatter(
    segments.head(20), x='avg_fico', y='default_rate_pct',
    size='total_loans', color='dti_segment',
    hover_data=['fico_segment', 'income_segment', 'total_loans'],
    labels={'avg_fico': 'Avg FICO Score', 'default_rate_pct': 'Default Rate (%)', 'dti_segment': 'DTI Segment'},
    title='Top 20 Segments by Default Rate (bubble size = loan count)'
)
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# --- Vintage analysis ---
st.subheader('Vintage Analysis — Default Rate by Origination Year & Grade')
vintage_agg = (
    vintage.groupby(['issue_year', 'risk_grade'], as_index=False)
    .agg(loans_originated=('loans_originated', 'sum'), defaults=('defaults', 'sum'))
)
vintage_agg['default_rate_pct'] = (vintage_agg['defaults'] / vintage_agg['loans_originated'] * 100).round(2)

fig4 = px.line(
    vintage_agg, x='issue_year', y='default_rate_pct', color='risk_grade', markers=True,
    labels={'issue_year': 'Origination Year', 'default_rate_pct': 'Default Rate (%)', 'risk_grade': 'Grade'},
    title='Default Rate by Vintage Year and Risk Grade'
)
st.plotly_chart(fig4, use_container_width=True)

st.divider()

with st.expander('Grade Performance Table'):
    st.dataframe(
        grade_perf[[
            'risk_grade', 'total_loans', 'avg_loan_amount', 'avg_interest_rate_pct',
            'avg_fico_score', 'avg_dti', 'default_rate_pct', 'avg_loss_given_default', 'expected_loss_rate'
        ]].rename(columns={
            'risk_grade': 'Grade', 'total_loans': 'Loans', 'avg_loan_amount': 'Avg Loan ($)',
            'avg_interest_rate_pct': 'Avg Rate (%)', 'avg_fico_score': 'Avg FICO', 'avg_dti': 'Avg DTI',
            'default_rate_pct': 'Default Rate (%)', 'avg_loss_given_default': 'Avg LGD',
            'expected_loss_rate': 'Expected Loss'
        }),
        use_container_width=True
    )
