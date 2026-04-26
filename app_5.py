import streamlit as st
import pandas as pd 
import plotly.express as px
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import re
from datetime import datetime

# --- 1. SETTINGS & EXECUTIVE THEME ---
st.set_page_config(page_title="Executive Fraud Intelligence", page_icon="🛡️", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 3.5rem !important; padding-bottom: 2rem; padding-left: 3rem; padding-right: 3rem; }
    .centered-title { text-align: center; width: 100%; color: #003366; font-size: 1.8rem; font-weight: 800; margin-bottom: 0.2rem; }
    .sub-header-text { text-align: center; width: 100%; color: #666; font-size: 1rem; margin-bottom: 1.5rem; }
    span[data-baseweb="tag"] { display: none !important; }
    div[data-baseweb="select"] > div:first-child { height: 42px !important; }
    label { font-size: 0.85rem !important; font-weight: 600; color: #444; }
    h3 { font-size: 1.1rem !important; font-weight: bold !important; color: #1f2937; border-bottom: 1px solid #eee; margin-bottom: 5px !important; }
    .chart-subtitle { font-size: 0.8rem; color: #777; margin-bottom: 10px; font-style: italic; }
</style>
""", unsafe_allow_html=True)

# --- 2. LOAD DATA ---
@st.cache_data
def load_data():
    df = pd.read_csv('Master_File_2020_Filtered.csv')
    df['date'] = pd.to_datetime(df['date'])
    if df['date'].dt.tz is not None:
        df['date'] = df['date'].dt.tz_localize(None)
    df = df[df['date'] <= datetime.now()]
    df['primary_tag'] = df['primary_tag'].fillna('Uncategorized').str.title().str.strip()
    df['source'] = df['source'].str.upper().str.strip()
    return df

df = load_data()

# --- 3. EXECUTIVE HEADER ---
st.markdown('<div class="centered-title">STRATEGIC FRAUD INTELLIGENCE & VELOCITY ANALYSIS</div>', unsafe_allow_html=True)

tab1, tab2 = st.tabs(["📊 Executive Dashboard", "🔍 Source Audit"])

with tab1:
    row1_col1, row1_col2 = st.columns([1, 1])
    row2_col1, row2_col2, row2_col3, row2_col4 = st.columns([0.7, 1, 1, 1])

    with row2_col1:
        st.subheader("🛡️ Strategic Filters")
        timeframe = st.selectbox("Interval", ["Weekly", "Monthly", "Quarterly", "Yearly"], index=1)
        all_tags = sorted(df['primary_tag'].unique())
        default_tags = [t for t in all_tags if t.lower() != 'other']
        selected_sources = st.multiselect("Sources", sorted(df['source'].unique()), default=list(df['source'].unique()))
        selected_tags = st.multiselect("Categories", all_tags, default=default_tags)

    # --- LOGIC ENGINE ---
    tf_map = {"Weekly": "W-SUN", "Monthly": "ME", "Quarterly": "QE", "Yearly": "YE"}
    window_map = {"Weekly": 10, "Monthly": 12, "Quarterly": 8, "Yearly": 999}
    label_map = {"Weekly": "Last 10 Weeks", "Monthly": "Last 12 Months", "Quarterly": "Last 8 Quarters", "Yearly": "Historical Overview"}

    mask = (df['source'].isin(selected_sources)) & (df['primary_tag'].isin(selected_tags))
    full_filtered = df[mask].copy()

    if not full_filtered.empty:
        temp_grouped = full_filtered.groupby(pd.Grouper(key='date', freq=tf_map[timeframe])).size()
        valid_periods = temp_grouped[temp_grouped.index <= datetime.now()].tail(window_map[timeframe]).index
        filtered_df = full_filtered[(full_filtered['date'] >= valid_periods[0]) & (full_filtered['date'] <= valid_periods[-1])]
    else: filtered_df = full_filtered

    # --- ROW 1: TRENDS & ACCELERATION ---
    with row1_col1:
        st.subheader(f"Historical Intelligence Trend Analysis ({timeframe})")
        st.markdown(f'<p class="chart-subtitle">Analyzing volume trends for the {label_map[timeframe]}</p>', unsafe_allow_html=True)
        if not filtered_df.empty:
            v_data = filtered_df.groupby([pd.Grouper(key='date', freq=tf_map[timeframe]), 'primary_tag']).size().reset_index(name='count')
            fig_v = px.line(v_data, x='date', y='count', color='primary_tag', markers=True, height=300, template="plotly_white")
            fig_v.update_layout(margin=dict(l=0, r=0, t=30, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10), title=None))
            st.plotly_chart(fig_v, use_container_width=True)
    with row1_col2:
        st.subheader(f"({timeframe}) Percent Change in Fraud Categories")
        
        # 1. Group the data based on the selected timeframe
        pivot = full_filtered.groupby([pd.Grouper(key='date', freq=tf_map[timeframe]), 'primary_tag']).size().unstack(fill_value=0)
        
        # 2. Safety Check: Filter out periods with zero data (trailing empty buckets)
        pivot = pivot[pivot.sum(axis=1) > 0]

        if len(pivot) > 1:
            if timeframe == "Yearly":
                # --- YTD Comparison (e.g., Jan-Apr 2025 vs Jan-Apr 2026) ---
                latest_date = full_filtered['date'].max()
                day_of_year = latest_date.timetuple().tm_yday
                
                curr_data = full_filtered[full_filtered['date'].dt.year == latest_date.year]
                prev_data = full_filtered[(full_filtered['date'].dt.year == latest_date.year - 1) & 
                                        (full_filtered['date'].dt.dayofyear <= day_of_year)]
                
                curr_vals = curr_data.groupby('primary_tag').size()
                prev_vals = prev_data.groupby('primary_tag').size().reindex(curr_vals.index, fill_value=0)
                label_text = f"YTD: Jan-{latest_date.strftime('%b')} {latest_date.year-1} vs {latest_date.year}"

            elif timeframe == "Quarterly":
                # --- Step-Back: Last FULL Quarter vs Previous FULL Quarter ---
                # If data is in Q2, compare Q1 2026 vs Q4 2025
                idx = -2 if len(pivot) >= 3 else -1
                prev_idx = -3 if len(pivot) >= 3 else -2
                
                curr_vals = pivot.iloc[idx]
                prev_vals = pivot.iloc[prev_idx]
                
                c_p, p_p = pivot.index[idx], pivot.index[prev_idx]
                label_text = f"Full Qtr: Q{(p_p.month-1)//3+1} {p_p.year} vs Q{(c_p.month-1)//3+1} {c_p.year}"

            elif timeframe == "Monthly":
                # --- Step-Back: Last Full Month vs Month Before ---
                # If today is April, compare March vs February
                idx = -2 if len(pivot) >= 3 else -1
                prev_idx = -3 if len(pivot) >= 3 else -2
                
                curr_vals = pivot.iloc[idx]
                prev_vals = pivot.iloc[prev_idx]
                label_text = f"Monthly: {pivot.index[prev_idx].strftime('%b %y')} vs {pivot.index[idx].strftime('%b %y')}"

            else: # Weekly
                # --- Step-Back: Last Full Week vs Week Before ---
                idx = -2 if len(pivot) >= 3 else -1
                prev_idx = -3 if len(pivot) >= 3 else -2
                
                curr_vals = pivot.iloc[idx]
                prev_vals = pivot.iloc[prev_idx]
                
                c_start = pivot.index[idx]
                p_start = pivot.index[prev_idx]
                label_text = f"Weekly: {p_start.strftime('%b %d')} vs {c_start.strftime('%b %d')}"

            # --- 3. Calculation Logic ---
            diff_df = (curr_vals - prev_vals).reset_index(name='Change')
            # Use 1 instead of 0 for denominator to prevent Infinity errors
            denom = prev_vals.replace(0, 1).values
            diff_df['%'] = (diff_df['Change'] / denom * 100)
            
            st.markdown(f"**{label_text}**")

            # --- 4. Visualization ---
            fig_a = px.bar(
                diff_df, x='primary_tag', y='Change', color='Change', 
                text=diff_df['%'].apply(lambda x: f"{x:+.0f}%"), 
                height=350, template="plotly_white", 
                color_continuous_scale='RdBu_r'
            )

            fig_a.update_traces(textposition='outside', cliponaxis=False)
            
            # Dynamic Y-axis headroom for labels
            y_max = diff_df['Change'].max()
            y_min = diff_df['Change'].min()
            fig_a.update_yaxes(range=[y_min * 1.4 if y_min < 0 else -2, y_max * 1.4 if y_max > 0 else 2])
            
            fig_a.update_layout(
                margin=dict(l=10, r=10, t=30, b=80), 
                coloraxis_showscale=False, 
                xaxis_title=None,
                yaxis_title="Delta (Article Count)"
            )
            fig_a.update_xaxes(tickangle=45)
            st.plotly_chart(fig_a, use_container_width=True)
        else:
            st.info("Awaiting more historical data to calculate deltas.")
            # --- ROW 2: SMALLER DONUT & WORD CLOUD ---
    with row2_col2:
        st.subheader("🌐 Source Distribution")
        if not filtered_df.empty:
            source_counts = filtered_df['source'].value_counts().reset_index()
            source_counts.columns = ['Source', 'Count']
            
            # Reduced height to 220 and increased hole to 0.5 for a 'lighter' look
            fig_pie = px.pie(
                source_counts, 
                values='Count', 
                names='Source', 
                hole=0.3, 
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            
            fig_pie.update_traces(
                textposition='auto', 
                textinfo='percent+label', 
                textfont_size=9, # Slightly smaller font to match smaller chart
                insidetextorientation='horizontal'
            )
            
            # Larger margins to center the smaller donut
            fig_pie.update_layout(
                margin=dict(l=10, r=10, t=5, b=5), 
                height=220, 
                showlegend=False
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    with row2_col3:
        st.subheader("Top 10 Fraud Types")
        top_tags = filtered_df['primary_tag'].value_counts().head(10).reset_index()
        fig_b = px.bar(top_tags, x='count', y='primary_tag', orientation='h', height=260, color_discrete_sequence=['#003366'])
        fig_b.update_layout(margin=dict(l=0, r=0, t=0, b=0), yaxis={'categoryorder':'total ascending'}, xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig_b, use_container_width=True)

    with row2_col4:
        st.subheader("Narrative Themes")
        final_stops = set(STOPWORDS)
        noise_words = ["say", "says", "said", "term", "terms", "according", "will", 
                       "also", "new", "used", "use", "using", "one", "many", "across", 
                       "including", "within", "without", "provide", "report", "article", 
                       "scammer", "scammers","scams", "fraud", "usaa", "payment", "payments",
                         "continue", "tool", "identity", "platform", "platforms", 
                         "companies", "help", "information", "system", "systems", "account", 
                         "bank", "form", "data", "member", "customer", "customers", "users", 
                         "user", "company", "consumer", "consumers", "attack", "attacks", 
                         "transaction", "transactions", "services", "security", "access",
                           "activity", "working", "time", "need", "make", "even", "first", 
                           "way", "people","often", "pymnts","financial", "victim","fraudsters",
                           "banks","sharing","real","fbi","schemes","alert","treasury","us",
                           "employee","news","attacker", "institution", "device","trust",
                           "risk","today","day","ftc","threat","year","victims","find","fake",
                           "what'","may","found","scam","bleeping computer","see"]
        final_stops.update([word.lower() for word in noise_words])

        raw_text = " ".join(filtered_df['body'].dropna().astype(str))
        cleaned = re.sub(r"\b[sS]\b", "", raw_text)      
        cleaned = re.sub(r"'s\b", "", cleaned)           
        cleaned = re.sub(r"'S\b", "", cleaned)           
        cleaned = cleaned.lower()

        if len(cleaned) > 20:
            wc = WordCloud(
                stopwords=final_stops,
                width=400, height=260, 
                background_color='white', 
                colormap='Dark2',
                max_words=50, 
                collocations=False
            ).generate(cleaned)
            
            fig_wc, ax = plt.subplots(figsize=(4, 2.6))
            ax.imshow(wc, interpolation='bilinear')
            ax.axis('off')
            st.pyplot(fig_wc)

with tab2:
    st.markdown("### 🔍 Source Integrity & Audit Report")
    
    # --- 1. Source Bias Matrix ---
    st.markdown("#### **Category Distribution by Source**")
    bias_df = df.groupby(['primary_tag', 'source']).size().unstack(fill_value=0)
    bias_df['Total'] = bias_df.sum(axis=1)
    st.dataframe(bias_df.sort_values('Total', ascending=False), use_container_width=True)
    
    st.divider()

    # --- 2. Audit Report and Global Metric ---
    col_audit, col_metric = st.columns([0.7, 0.3])
    
    with col_audit:
        st.markdown("#### **Intelligence Source Audit**")
        
        audit_data = []
        for source in df['source'].unique():
            s_data = df[df['source'] == source]
            audit_data.append({
                "Source": source.upper(),
                "Article Count": len(s_data),
                "First Seen": s_data['date'].min().strftime('%Y-%m-%d'),
                "Last Seen": s_data['date'].max().strftime('%Y-%m-%d')
            })
        
        audit_df = pd.DataFrame(audit_data).sort_values('Article Count', ascending=False)
        st.table(audit_df)
    
    with col_metric:
        st.markdown("#### **Summary**")
        try:
            # Using a container to prevent the "Failed to fetch" error seen in your screenshot
            with st.container():
                st.metric(
                    label="Total Unique Leads", 
                    value=f"{len(df):,}"
                )
                st.info(f"Analysis covers **{len(df['source'].unique())}** verified intelligence vendors.")
        except Exception:
            # Fallback for the error seen in image_5cabf9.png
            st.write(f"**Total Unique Leads:** {len(df):,}")
            st.write(f"**Verified Vendors:** {len(df['source'].unique())}")

    
    