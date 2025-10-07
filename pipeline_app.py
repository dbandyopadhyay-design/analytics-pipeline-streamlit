"""
Social Media Analytics Pipeline App
Automated 5-step pipeline for BigQuery analytics with table existence checking
"""

import streamlit as st
import pandas as pd
import uuid
import logging
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from openai import OpenAI
import json
import os
import time
import plotly.express as px

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Analytics Pipeline - Automated",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'pipeline_status' not in st.session_state:
    st.session_state.pipeline_status = {}
if 'results' not in st.session_state:
    st.session_state.results = None
if 'processing' not in st.session_state:
    st.session_state.processing = False

class AnalyticsPipeline:
    def __init__(self):
        self.bq = None
        self.grok_client = None
        self.project_id = "x-gcp-lightbulb"
        self.dataset_id = "Insights_lab_reporting"
        
    def initialize_clients(self):
        """Initialize BigQuery and Grok API clients"""
        try:
            self.bq = bigquery.Client(project=self.project_id)
            
            # Try to get Grok API key from secrets or environment
            try:
                grok_api_key = st.secrets["GROK_API_KEY"]
            except:
                grok_api_key = os.getenv("GROK_API_KEY")
                
            if not grok_api_key:
                st.error("❌ Grok API key not found. Please set GROK_API_KEY in secrets or environment.")
                return False
                
            self.grok_client = OpenAI(
                api_key=grok_api_key,
                base_url="https://api.x.ai/v1",
                timeout=3600.0
            )
            return True
        except Exception as e:
            st.error(f"❌ Error initializing clients: {str(e)}")
            return False
    
    def calculate_date_range(self, date_mode, start_date=None, end_date=None, days_back=28):
        """Calculate effective start and end dates"""
        if date_mode == "specific" and start_date and end_date:
            return start_date, end_date
        else:
            # Rolling date window
            end_date = date.today() - timedelta(days=1)  # Yesterday
            start_date = end_date - timedelta(days=days_back-1)
            return start_date, end_date
    
    def generate_table_names(self, country, start_date, end_date, category_name):
        """Generate table names based on parameters"""
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        
        impressions_table = f"`{self.project_id}.{self.dataset_id}.posts_impressions_{country}_{start_str}_to_{end_str}`"
        tagged_table = f"`{self.project_id}.{self.dataset_id}.tagged_posts_{category_name}_{country}_{start_str}_to_{end_str}`"
        
        return impressions_table, tagged_table
    
    def table_exists(self, table_name):
        """Check if a BigQuery table exists"""
        try:
            # Remove backticks and project prefix for API call
            clean_table_name = table_name.strip('`')
            if clean_table_name.startswith(f'{self.project_id}.'):
                clean_table_name = clean_table_name[len(f'{self.project_id}.'):]
            
            table_ref = self.bq.dataset(self.dataset_id).table(clean_table_name.split('.')[-1])
            self.bq.get_table(table_ref)
            return True
        except:
            return False
    
    def step1_create_impressions_table(self, country, start_date, end_date):
        """Step 1: Create impressions table (with existence check)"""
        impressions_table, _ = self.generate_table_names(country, start_date, end_date, "temp")
        
        # Check if table already exists
        if self.table_exists(impressions_table):
            st.success(f"✅ Step 1: Impressions table already exists - {impressions_table}")
            return True, impressions_table
        
        st.info(f"🔄 Step 1: Creating impressions table for {country.upper()} ({start_date} to {end_date})")
        
        # Build the query from Query final.md Script 1
        query = f"""
        DECLARE effective_start_date DATE DEFAULT DATE('{start_date}');
        DECLARE effective_end_date DATE DEFAULT DATE('{end_date}');
        DECLARE country_param STRING DEFAULT '{country}';
        
        CREATE OR REPLACE TABLE {impressions_table} AS
        SELECT
            *,
            effective_start_date AS data_start_date,
            effective_end_date AS data_end_date
        FROM `twttr-bq-tweetsource-prod.user.unhydrated_flat` A
        INNER JOIN (
            SELECT
                LOWER(engagingIpCountry) Country,
                tweetid tweet_id,
                SUM(CASE WHEN interactionType = 0 THEN count ELSE 0 END) Impressions,
                SUM(CASE WHEN interactionType = 14 THEN count ELSE 0 END) Favorite,
                SUM(CASE WHEN interactionType = 27 THEN count ELSE 0 END) Reply,
                SUM(CASE WHEN interactionType = 29 THEN count ELSE 0 END) Retweet,
                SUM(CASE WHEN interactionType = 31 THEN count ELSE 0 END) Quote,
                SUM(CASE WHEN interactionType = 65 THEN count ELSE 0 END) Video_views
            FROM `twttr-bq-iesource-prod.user.tweet_interaction_daily_aggregates`
            WHERE
                TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN
                    TIMESTAMP(effective_start_date) AND TIMESTAMP(effective_end_date)
                AND interactionType IN (0, 14, 27, 29, 31, 65)
                AND LOWER(engagingIpCountry) = country_param
            GROUP BY 1, 2
        ) B
        ON A.tweetid = B.tweet_id
        WHERE
            TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN
                TIMESTAMP(effective_start_date) AND TIMESTAMP(effective_end_date)
        """
        
        try:
            job = self.bq.query(query)
            job.result()  # Wait for completion
            st.success(f"✅ Step 1: Impressions table created successfully - {impressions_table}")
            return True, impressions_table
        except Exception as e:
            st.error(f"❌ Step 1 failed: {str(e)}")
            return False, None
    
    def step2_upload_and_tag(self, template_df, country, start_date, end_date, category_name, impressions_table):
        """Step 2: Upload CSV and create tagged table"""
        st.info(f"🔄 Step 2: Uploading template and creating tagged posts table")
        
        # Create temporary template table
        temp_id = str(uuid.uuid4()).replace('-', '')[:8]
        temp_template_table = f"`{self.project_id}.{self.dataset_id}.temp_template_{category_name}_{temp_id}`"
        
        try:
            # Upload template to BigQuery
            template_df.to_gbq(
                destination_table=f"{self.project_id}.{self.dataset_id}.temp_template_{category_name}_{temp_id}",
                project_id=self.project_id,
                if_exists="replace",
                progress_bar=False
            )
            
            # Generate tagged table name
            _, tagged_table = self.generate_table_names(country, start_date, end_date, category_name)
            
            # Build tagging query from Query final.md Script 2
            query = f"""
            CREATE OR REPLACE TABLE {tagged_table} AS
            WITH
            total_table AS (
                SELECT
                    text, userScreenName, tweetId, hashtags, atMentionedScreenNames, entity_annotations.entityId AS eid,
                    Impressions, userId, Reply, Favorite, Retweet, Quote, Video_views, media, nsfwUser, data_start_date, data_end_date, Country
                FROM {impressions_table}
                LEFT JOIN UNNEST(hashtags) AS hashtags
                LEFT JOIN UNNEST(atMentionedScreenNames) AS atMentionedScreenNames
                LEFT JOIN UNNEST(entity_annotations) AS entity_annotations
            ),
            tags_table AS (
                SELECT `Sub Category`, REPLACE(tag, "@", "") AS tags, "Handles" AS tag_type 
                FROM {temp_template_table} 
                LEFT JOIN UNNEST(SPLIT(CAST(Handles AS STRING), ", ")) AS tag 
                WHERE Handles IS NOT NULL AND CAST(Handles AS STRING) NOT IN ('None', 'NULL', '') AND tag IS NOT NULL AND TRIM(tag) != ""
                UNION ALL
                SELECT `Sub Category`, LOWER(TRIM(tag)) AS tags, "Terms" AS tag_type 
                FROM {temp_template_table} 
                LEFT JOIN UNNEST(SPLIT(CAST(Terms AS STRING), ", ")) AS tag 
                WHERE Terms IS NOT NULL AND CAST(Terms AS STRING) NOT IN ('None', 'NULL', '') AND tag IS NOT NULL AND TRIM(tag) != ""
                UNION ALL
                SELECT `Sub Category`, REPLACE(tag, "#", "") AS tags, "Hashtags" AS tag_type 
                FROM {temp_template_table} 
                LEFT JOIN UNNEST(SPLIT(CAST(Hashtags AS STRING), ", ")) AS tag 
                WHERE Hashtags IS NOT NULL AND CAST(Hashtags AS STRING) NOT IN ('None', 'NULL', '') AND tag IS NOT NULL AND TRIM(tag) != ""
                UNION ALL
                SELECT `Sub Category`, TRIM(tag) AS tags, "Entity" AS tag_type 
                FROM {temp_template_table} 
                LEFT JOIN UNNEST(SPLIT(CAST(`Entity ID` AS STRING), ", ")) AS tag 
                WHERE `Entity ID` IS NOT NULL AND CAST(`Entity ID` AS STRING) NOT IN ('None', 'NULL', '') AND tag IS NOT NULL AND TRIM(tag) != ""
            ),
            tagged AS (
                SELECT tweetid, text, `Sub Category` AS tags, tag_type 
                FROM total_table 
                JOIN tags_table ON 
                    LOWER(total_table.hashtags) = LOWER(tags_table.tags) OR 
                    LOWER(total_table.atMentionedScreenNames) = LOWER(tags_table.tags) OR 
                    LOWER(total_table.userScreenName) = LOWER(tags_table.tags) OR 
                    LOWER(CAST(total_table.eid AS STRING)) = LOWER(tags_table.tags) 
                GROUP BY 1,2,3,4
                UNION ALL
                SELECT tweetid, text, `Sub Category` AS tags, "Terms" AS tag_type
                FROM {impressions_table} A, UNNEST(ML.NGRAMS(REGEXP_EXTRACT_ALL(LOWER(text), r'[a-z0-9]+'),[1,3],' ')) AS terms
                JOIN ( 
                    SELECT `Sub Category`, LOWER(TRIM(terms_tag)) AS terms_tag 
                    FROM {temp_template_table}, UNNEST(SPLIT(CAST(Terms AS STRING), ", ")) AS terms_tag 
                    WHERE Terms IS NOT NULL AND CAST(Terms AS STRING) NOT IN ('None', 'NULL', '') AND terms_tag IS NOT NULL AND TRIM(terms_tag) != ""
                ) B ON terms = B.terms_tag 
                GROUP BY 1,2,3,4
            )
            SELECT A.*, B.tags, B.tag_type
            FROM {impressions_table} A
            JOIN (SELECT tweetid AS uc, text AS uctext, STRING_AGG(DISTINCT tags) AS tags, STRING_AGG(DISTINCT tag_type) AS tag_type FROM tagged GROUP BY 1,2) B 
            ON A.tweetid = B.uc
            """
            
            job = self.bq.query(query)
            job.result()
            
            # Cleanup temporary template table
            try:
                cleanup_query = f"DROP TABLE IF EXISTS {temp_template_table}"
                self.bq.query(cleanup_query).result()
            except:
                pass
            
            st.success(f"✅ Step 2: Tagged posts table created - {tagged_table}")
            return True, tagged_table
            
        except Exception as e:
            st.error(f"❌ Step 2 failed: {str(e)}")
            return False, None
    
    def step3_run_analytics(self, tagged_table):
        """Step 3: Run final analytics query and return CSV data"""
        st.info("🔄 Step 3: Running comprehensive analytics query")
        
        # Build analytics query from Query final.md Script 3
        query = f"""
        WITH
        final_tagged_table AS (
            SELECT * FROM {tagged_table}
        ),
        aggregate_metrics AS (
            SELECT
                tags,
                COUNT(DISTINCT tweetId) AS count_of_posts,
                SUM(Impressions) AS impressions,
                SUM(Reply) AS reply,
                SUM(Favorite) AS favorite,
                SUM(Retweet) AS retweet,
                SUM(Video_views) AS video_views,
                COUNT(DISTINCT userid) AS no_of_authors
            FROM (
                SELECT
                    tags,
                    userid,
                    tweetId,
                    Impressions,
                    Reply,
                    Favorite,
                    Retweet,
                    Quote,
                    Video_views
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags
                WHERE
                    nsfwUser IS FALSE
                GROUP BY ALL 
            )
            GROUP BY tags 
        ),
        media_analytics AS (
            SELECT
                tags,
                TO_JSON_STRING(ARRAY_AGG(
                    IF(media_type = 1, STRUCT(tweetId, text, Impressions, media_url AS photo_url), NULL) IGNORE NULLS
                    ORDER BY Impressions DESC
                    LIMIT 5 
                )) AS top_photos,
                TO_JSON_STRING(ARRAY_AGG(
                    IF(media_type = 2, STRUCT(tweetId, text, Impressions, media_url AS photo_url), NULL) IGNORE NULLS
                    ORDER BY Impressions DESC
                    LIMIT 5 
                )) AS top_gifs
            FROM (
                SELECT
                    tags,
                    tweetId,
                    text,
                    m.media_type,
                    Impressions,
                    m.media_url
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags,
                    UNNEST(media) AS m
                WHERE
                    m.media_type IN (1, 2)
                    AND nsfwUser = FALSE 
            )
            GROUP BY tags 
        ),
        video_analytics AS (
            SELECT
                tags,
                TO_JSON_STRING(ARRAY_AGG( 
                    STRUCT( 
                        tweetId,
                        text,
                        Impressions,
                        url AS video_url,
                        photo_url,
                        Video_views AS video_views 
                    )
                    ORDER BY Video_views DESC
                    LIMIT 5 
                )) AS top_videos
            FROM (
                SELECT
                    tweetId,
                    text,
                    tags,
                    Impressions,
                    Video_views,
                    vv.url,
                    vv.bit_rate,
                    m.media_url AS photo_url,
                    RANK() OVER (PARTITION BY tweetId, text, tags, Video_views ORDER BY vv.bit_rate DESC) AS rank
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags,
                    UNNEST(media) AS m,
                    UNNEST(m.video_variants) AS vv
                WHERE
                    m.media_type = 3
                    AND nsfwUser = FALSE
                QUALIFY rank = 1 
            )
            GROUP BY tags 
        ),
        age_demographics AS (
            SELECT
                tags,
                STRING_AGG(CONCAT(user_age_bucket, " ", percentage)) AS age_split
            FROM (
                SELECT
                    tags,
                    REPLACE(REPLACE(user_age_bucket, "Age", ""), "To", "-") AS user_age_bucket,
                    COUNT(DISTINCT A.userId) AS user_count,
                    ROUND(100.0 * COUNT(DISTINCT A.userId) / SUM(COUNT(DISTINCT A.userId)) OVER (PARTITION BY tags), 2) AS percentage
                FROM
                    final_tagged_table A,
                    UNNEST(SPLIT(tags)) AS tags
                INNER JOIN (
                    SELECT
                        user_id,
                        user_age_bucket
                    FROM
                        `x-gcp-lightbulb.usersource.usersource_mau_country_all_raw_annotated`
                    WHERE
                        user_age_bucket IS NOT NULL
                        AND user_age_bucket NOT IN ("Age13To17")
                    GROUP BY user_id, user_age_bucket 
                ) B
                ON A.userId = B.user_id
                WHERE nsfwUser IS FALSE
                GROUP BY tags, user_age_bucket
                ORDER BY tags, user_age_bucket 
            )
            GROUP BY tags 
        ),
        gender_demographics AS (
            SELECT
                tags,
                STRING_AGG(CONCAT(user_gender, " ", percentage)) AS gender_split
            FROM (
                SELECT
                    tags,
                    user_gender,
                    COUNT(DISTINCT A.userId) AS user_count,
                    ROUND(100.0 * COUNT(DISTINCT A.userId) / SUM(COUNT(DISTINCT A.userId)) OVER (PARTITION BY tags), 2) AS percentage
                FROM
                    final_tagged_table A,
                    UNNEST(SPLIT(tags)) AS tags
                INNER JOIN (
                    SELECT
                        user_id,
                        user_gender
                    FROM
                        `x-gcp-lightbulb.usersource.usersource_mau_country_all_raw_annotated`
                    WHERE
                        user_gender IN ('MALE', 'FEMALE')
                    GROUP BY user_id, user_gender 
                ) B
                ON A.userId = B.user_id
                WHERE nsfwUser IS FALSE
                GROUP BY tags, user_gender
                ORDER BY tags, user_gender 
            )
            GROUP BY tags 
        ),
        vertical_demographics AS (
            SELECT
                tags,
                STRING_AGG(vertical ORDER BY user_count DESC LIMIT 5) AS vertical_split
            FROM (
                SELECT
                    tags,
                    vertical,
                    COUNT(DISTINCT A.userId) AS user_count
                FROM
                    final_tagged_table A,
                    UNNEST(SPLIT(tags)) AS tags
                INNER JOIN (
                    SELECT
                        user_id,
                        voi.sub_vertical AS vertical
                    FROM
                        `x-gcp-lightbulb.usersource.usersource_mau_country_all_raw_annotated`,
                        UNNEST(voi) AS voi
                    WHERE
                        voi.sub_vertical IS NOT NULL
                        AND LOWER(voi.sub_vertical) NOT LIKE '%other%'
                    GROUP BY user_id, vertical 
                ) B
                ON A.userId = B.user_id
                WHERE nsfwUser IS FALSE
                GROUP BY tags, vertical
                ORDER BY tags, vertical 
            )
            GROUP BY tags 
        ),
        hashtag_analytics AS (
            SELECT
                tags,
                STRING_AGG(ht ORDER BY impressions DESC LIMIT 10) AS top_hashtags
            FROM (
                SELECT
                    tags,
                    ht,
                    SUM(Impressions) AS impressions
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags,
                    UNNEST(hashtags) AS ht
                WHERE nsfwUser IS FALSE
                GROUP BY tags, ht 
            )
            GROUP BY tags 
        ),
        mentions_analytics AS (
            SELECT
                tags,
                STRING_AGG(mentions ORDER BY impressions DESC LIMIT 10) AS top_mentions
            FROM (
                SELECT
                    tags,
                    atMentionedScreenNames AS mentions,
                    SUM(Impressions) AS impressions
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags,
                    UNNEST(atMentionedScreenNames) AS atMentionedScreenNames
                WHERE nsfwUser IS FALSE
                GROUP BY tags, atMentionedScreenNames 
            )
            GROUP BY tags 
        ),
        entity_analytics AS (
            SELECT
                tags,
                STRING_AGG(name ORDER BY uc DESC LIMIT 10) AS entity_names
            FROM (
                SELECT
                    tags,
                    source_name AS name,
                    SUM(Impressions) AS impressions,
                    COUNT(DISTINCT tweetId) AS uc
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags,
                    UNNEST(entity_annotations) AS et
                JOIN (
                    SELECT
                        source_id,
                        source_name
                    FROM
                        `x-gcp-lightbulb.Insights_lab_reporting.flattened_entities_relationships_agg`
                    GROUP BY source_id, source_name 
                ) A
                ON A.source_id = et.entityid
                WHERE nsfwUser IS FALSE
                GROUP BY tags, source_name 
            )
            GROUP BY tags 
        ),
        creator_analytics AS (
            SELECT
                tags,
                STRING_AGG(creator ORDER BY impressions DESC LIMIT 10) AS top_creators
            FROM (
                SELECT
                    tags,
                    userScreenName AS creator,
                    SUM(Impressions) AS impressions
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags
                WHERE nsfwUser IS FALSE
                GROUP BY tags, userScreenName 
            )
            GROUP BY tags 
        ),
        post_analytics AS (
            SELECT
                tags,
                TO_JSON_STRING(ARRAY_AGG( 
                    STRUCT(tweetId, text, Impressions)
                    ORDER BY Impressions DESC
                    LIMIT 5 
                )) AS top_posts,
                TO_JSON_STRING(ARRAY_AGG( 
                    STRUCT(tweetId, text, Reply)
                    ORDER BY Reply DESC
                    LIMIT 5 
                )) AS top_posts_reply,
                TO_JSON_STRING(ARRAY_AGG( 
                    STRUCT(tweetId, text, Favorite)
                    ORDER BY Favorite DESC
                    LIMIT 5 
                )) AS top_posts_favorite,
                TO_JSON_STRING(ARRAY_AGG( 
                    STRUCT(tweetId, text, Retweet)
                    ORDER BY Retweet DESC
                    LIMIT 5 
                )) AS top_posts_retweet,
                TO_JSON_STRING(ARRAY_AGG( 
                    STRUCT(tweetId, text, Quote)
                    ORDER BY Quote DESC
                    LIMIT 5 
                )) AS top_posts_quote
            FROM (
                SELECT
                    tags,
                    text,
                    tweetId,
                    Impressions,
                    Reply,
                    Favorite,
                    Retweet,
                    Quote
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tags
                WHERE nsfwUser IS FALSE
                GROUP BY tags, text, tweetId, Impressions, Reply, Favorite, Retweet, Quote 
            )
            GROUP BY tags 
        ),
        report_details AS (
            SELECT
                tags,
                country,
                data_start_date,
                data_end_date
            FROM
                final_tagged_table,
                UNNEST(SPLIT(tags)) AS tags
            GROUP BY 1, 2, 3, 4 
        ),
        line_graph_analytics AS (
            WITH
            DailyPostCounts AS (
                SELECT
                    DATE(ts) AS post_date,
                    tag,
                    COUNT(DISTINCT tweetid) AS no_of_posts
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tag
                GROUP BY 1, 2
            ),
            TopPostPerDay AS (
                SELECT
                    DATE(ts) AS post_date,
                    tag,
                    STRUCT(tweetId, text, Impressions) AS top_post
                FROM
                    final_tagged_table,
                    UNNEST(SPLIT(tags)) AS tag
                QUALIFY ROW_NUMBER() OVER(PARTITION BY DATE(ts), tag ORDER BY Impressions DESC) = 1
            )
            SELECT
                counts.tag AS tags,
                TO_JSON_STRING(
                    ARRAY_AGG(
                        STRUCT(
                            counts.post_date AS date,
                            counts.no_of_posts AS post_count,
                            top.top_post AS top_post_day
                        )
                        ORDER BY counts.post_date ASC
                    )
                ) AS daily_data_json
            FROM
                DailyPostCounts AS counts
            LEFT JOIN
                TopPostPerDay AS top
                ON counts.post_date = top.post_date AND counts.tag = top.tag
            GROUP BY 1
        )
        SELECT
            COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags, ha.tags, men.tags, ea.tags, ca.tags, pa.tags, rp.tags, lga.tags) AS tags,
            am.count_of_posts,
            am.impressions,
            am.reply,
            am.favorite,
            am.retweet,
            am.video_views,
            am.no_of_authors,
            ma.top_photos,
            ma.top_gifs,
            va.top_videos,
            ad.age_split,
            gd.gender_split,
            vd.vertical_split AS vertical_of_interest,
            ha.top_hashtags,
            men.top_mentions,
            ea.entity_names,
            ca.top_creators,
            pa.top_posts,
            pa.top_posts_reply,
            pa.top_posts_favorite,
            pa.top_posts_retweet,
            pa.top_posts_quote,
            lga.daily_data_json,
            rp.country,
            rp.data_start_date,
            rp.data_end_date
        FROM aggregate_metrics am
        FULL OUTER JOIN media_analytics ma ON am.tags = ma.tags
        FULL OUTER JOIN video_analytics va ON COALESCE(am.tags, ma.tags) = va.tags
        FULL OUTER JOIN age_demographics ad ON COALESCE(am.tags, ma.tags, va.tags) = ad.tags
        FULL OUTER JOIN gender_demographics gd ON COALESCE(am.tags, ma.tags, va.tags, ad.tags) = gd.tags
        FULL OUTER JOIN vertical_demographics vd ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags) = vd.tags
        FULL OUTER JOIN hashtag_analytics ha ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags) = ha.tags
        FULL OUTER JOIN mentions_analytics men ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags, ha.tags) = men.tags
        FULL OUTER JOIN entity_analytics ea ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags, ha.tags, men.tags) = ea.tags
        FULL OUTER JOIN creator_analytics ca ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags, ha.tags, men.tags, ea.tags) = ca.tags
        FULL OUTER JOIN post_analytics pa ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags, ha.tags, men.tags, ea.tags, ca.tags) = pa.tags
        FULL OUTER JOIN report_details rp ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags, ha.tags, men.tags, ea.tags, ca.tags, pa.tags) = rp.tags
        FULL OUTER JOIN line_graph_analytics lga ON COALESCE(am.tags, ma.tags, va.tags, ad.tags, gd.tags, vd.tags, ha.tags, men.tags, ea.tags, ca.tags, pa.tags, rp.tags) = lga.tags
        ORDER BY tags
        """
        
        try:
            analytics_df = pd.read_gbq(
                query,
                project_id=self.project_id,
                progress_bar_type=None
            )
            
            st.success(f"✅ Step 3: Analytics completed! Found {len(analytics_df)} brands with data")
            return True, analytics_df
            
        except Exception as e:
            st.error(f"❌ Step 3 failed: {str(e)}")
            return False, None
    
    def step4_generate_ai_summaries(self, analytics_df, batch_size=5):
        """Step 4: Generate AI summaries using Grok"""
        st.info("🔄 Step 4: Generating AI summaries with Grok")
        
        summaries = []
        
        def _get_sample_posts(json_str, limit=2):
            """Extract sample post texts from JSON string"""
            try:
                import json
                posts = json.loads(json_str) if json_str and json_str != '[]' else []
                return [{'text': post.get('text', '')[:100] + '...' if len(post.get('text', '')) > 100 else post.get('text', ''), 
                        'impressions': post.get('Impressions', 0)} for post in posts[:limit]]
            except:
                return []
        
        for i in range(0, len(analytics_df), batch_size):
            batch = analytics_df.iloc[i:i+batch_size]
            
            try:
                # Prepare data for Grok
                batch_data = []
                for _, row in batch.iterrows():
                    row_data = {
                        'category': row['tags'],
                        'posts': int(row.get('count_of_posts', 0)),
                        'impressions': int(row.get('impressions', 0)),
                        'engagement': {
                            'replies': int(row.get('reply', 0)),
                            'favorites': int(row.get('favorite', 0)),
                            'retweets': int(row.get('retweet', 0)),
                            'video_views': int(row.get('video_views', 0))
                        },
                        'demographics': {
                            'age_split': row.get('age_split', ''),
                            'gender_split': row.get('gender_split', ''),
                            'vertical_of_interest': row.get('vertical_of_interest', '')
                        },
                        'content': {
                            'top_hashtags': row.get('top_hashtags', ''),
                            'top_mentions': row.get('top_mentions', ''),
                            'entity_names': row.get('entity_names', ''),
                            'top_creators': row.get('top_creators', '')
                        },
                        'top_posts_sample': _get_sample_posts(row.get('top_posts', '[]'), 2),
                        'top_videos_sample': _get_sample_posts(row.get('top_videos', '[]'), 1),
                        'top_photos_sample': _get_sample_posts(row.get('top_photos', '[]'), 1)
                    }
                    batch_data.append(row_data)
                
                # Create prompt for Grok
                prompt = f"""
                Analyze the following comprehensive social media analytics data and provide insights for each category/topic:

                {json.dumps(batch_data, indent=2)}

                For each category (which could be brands, industry segments, topics, or thematic areas), provide a 2-3 sentence summary covering:
                1. Key engagement patterns and performance metrics (posts, impressions, engagement rates)
                2. Content themes and audience insights (hashtags, mentions, demographics, interests)
                3. Notable trends, top-performing content, or strategic opportunities

                Use the provided data including:
                - Engagement metrics (replies, favorites, retweets, video views)
                - Demographics (age, gender, interests)
                - Content analysis (hashtags, mentions, entities, creators)
                - Sample top posts, videos, and photos with their performance

                Return a JSON object where keys are the category names and values are the analytical summary strings.
                """
                
                # Call Grok API
                response = self.grok_client.chat.completions.create(
                    model="grok-3",
                    messages=[
                        {"role": "system", "content": "You are an expert social media analyst. Provide concise, actionable insights based on analytics data."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3
                )
                
                # Parse response
                try:
                    grok_response = response.choices[0].message.content
                    
                    if "```json" in grok_response:
                        json_start = grok_response.find("```json") + 7
                        json_end = grok_response.find("```", json_start)
                        grok_response = grok_response[json_start:json_end].strip()
                    elif "```" in grok_response:
                        json_start = grok_response.find("```") + 3
                        json_end = grok_response.find("```", json_start)
                        grok_response = grok_response[json_start:json_end].strip()
                    
                    summaries_dict = json.loads(grok_response)
                    
                    for _, row in batch.iterrows():
                        brand_name = row['tags']
                        
                        # Check for exact match first
                        if brand_name in summaries_dict:
                            summary = summaries_dict[brand_name]
                        else:
                            # Try case-insensitive match
                            for key in summaries_dict.keys():
                                if key.lower().strip() == brand_name.lower().strip():
                                    summary = summaries_dict[key]
                                    break
                            else:
                                summary = "No AI summary available for this category."
                        
                        summaries.append(summary)
                        
                except (json.JSONDecodeError, KeyError, IndexError):
                    for _ in range(len(batch)):
                        summaries.append("No AI summary available for this category.")
                        
            except Exception as e:
                logger.error(f"Error generating AI summaries for batch: {str(e)}")
                for _ in range(len(batch)):
                    summaries.append("AI summary generation failed.")
        
        analytics_df['ai_summary'] = summaries[:len(analytics_df)]
        st.success("✅ Step 4: AI summaries generated successfully")
        return analytics_df
    
    def step5_generate_html_report(self, df, category_name):
        """Step 5: Generate HTML report using the playground template structure"""
        st.info("🔄 Step 5: Generating HTML dashboard")
        
        # Check if DataFrame is empty
        if df.empty:
            st.error("❌ Cannot generate HTML report: No data available")
            return self._get_fallback_html_template()
        
        # Extract dynamic header information
        country_names = {
            'fr': 'France', 'us': 'United States', 'gb': 'United Kingdom', 
            'de': 'Germany', 'es': 'Spain', 'it': 'Italy', 'ca': 'Canada',
            'au': 'Australia', 'jp': 'Japan', 'br': 'Brazil', 'mx': 'Mexico'
        }
        
        # Safely extract values with fallbacks
        country = 'global'
        start_date = ''
        end_date = ''
        
        if 'country' in df.columns and len(df) > 0:
            country = df['country'].iloc[0] if pd.notna(df['country'].iloc[0]) else 'global'
        if 'data_start_date' in df.columns and len(df) > 0:
            start_date = df['data_start_date'].iloc[0] if pd.notna(df['data_start_date'].iloc[0]) else ''
        if 'data_end_date' in df.columns and len(df) > 0:
            end_date = df['data_end_date'].iloc[0] if pd.notna(df['data_end_date'].iloc[0]) else ''
        
        # Format header information
        country_display = country_names.get(country.lower(), country.upper())
        date_range = f"{start_date} to {end_date}" if start_date and end_date else "Custom Analysis"
        header_title = f"{category_name} Analysis"
        header_subtitle = f"X Insights Lab | {country_display} | {date_range}"
        
        # Convert DataFrame to JavaScript format matching playground structure
        js_data = []
        for _, row in df.iterrows():
            # Parse demographics
            def parse_demographics(demo_str):
                if pd.isna(demo_str) or not demo_str:
                    return []
                items = []
                for item in demo_str.split(','):
                    if ' ' in item.strip():
                        parts = item.strip().rsplit(' ', 1)
                        if len(parts) == 2:
                            label, value = parts
                            try:
                                items.append({'label': label.strip(), 'value': float(value)})
                            except:
                                pass
                return items
            
            # Safely extract values with fallbacks
            def safe_int(value, default=0):
                try:
                    return int(value) if pd.notna(value) and value != '' else default
                except (ValueError, TypeError):
                    return default
            
            def safe_get(row, key, default=''):
                try:
                    return row.get(key, default) if key in row else default
                except:
                    return default
            
            brand_data = {
                'name': safe_get(row, 'tags', 'Unknown'),
                'impressions': safe_int(safe_get(row, 'impressions')),
                'posts': safe_int(safe_get(row, 'count_of_posts')),
                'engagement': safe_int(safe_get(row, 'reply')) + safe_int(safe_get(row, 'favorite')) + safe_int(safe_get(row, 'retweet')),
                # Enhanced engagement metrics
                'reply': safe_int(safe_get(row, 'reply')),
                'favorite': safe_int(safe_get(row, 'favorite')),
                'retweet': safe_int(safe_get(row, 'retweet')),
                'video_views': safe_int(safe_get(row, 'video_views')),
                # NEW FIELDS - Authors and Daily Data
                'no_of_authors': safe_int(safe_get(row, 'no_of_authors')),
                'daily_data_json': self._parse_json_field(safe_get(row, 'daily_data_json', '[]')),
                # Demographics
                'age_data': parse_demographics(safe_get(row, 'age_split')),
                'gender_data': parse_demographics(safe_get(row, 'gender_split')),
                # Media content
                'top_photos': safe_get(row, 'top_photos', '[]'),
                'top_gifs': safe_get(row, 'top_gifs', '[]'), 
                'top_videos': safe_get(row, 'top_videos', '[]'),
                # Post analytics
                'top_posts': safe_get(row, 'top_posts', '[]'),
                'top_posts_reply': safe_get(row, 'top_posts_reply', '[]'),
                'top_posts_favorite': safe_get(row, 'top_posts_favorite', '[]'),
                'top_posts_retweet': safe_get(row, 'top_posts_retweet', '[]'),
                'top_posts_quote': safe_get(row, 'top_posts_quote', '[]'),
                # Social analytics
                'top_hashtags': safe_get(row, 'top_hashtags'),
                'top_mentions': safe_get(row, 'top_mentions'),
                'entity_names': safe_get(row, 'entity_names'),
                'top_creators': safe_get(row, 'top_creators'),
                'verticals': safe_get(row, 'vertical_of_interest'),  # Match playground structure
                'ai_summary': safe_get(row, 'ai_summary') if pd.notna(safe_get(row, 'ai_summary')) else ''
            }
            js_data.append(brand_data)
        
        js_data_str = json.dumps(js_data, ensure_ascii=False, indent=12)
        
        # Debug: Show data structure
        st.write(f"📊 Generated data for {len(js_data)} categories")
        st.write(f"🔍 Sample data keys: {list(js_data[0].keys()) if js_data else 'No data'}")
        
        # Read the playground HTML template and customize it
        try:
            with open('/Users/dbandyopadhyay/Desktop/Insights Lab template - v.2 - full /playground_analytics_dashboard.html', 'r', encoding='utf-8') as f:
                html_template = f.read()
            st.write("✅ Successfully loaded playground template")
        except Exception as e:
            st.warning(f"⚠️ Could not load playground template: {e}")
            # Fallback if file not found - use embedded template
            html_template = self._get_fallback_html_template()
        
        # Replace placeholders in the template - more robust approach
        html_content = html_template
        
        # Replace header information
        html_content = html_content.replace('Football Analytics Analysis', header_title)
        html_content = html_content.replace('X Insights Lab | France | 2025-09-21 to 2025-09-23', header_subtitle)
        html_content = html_content.replace('5 Categories', f'{len(df)} Categories')
        # Safely calculate total posts
        total_posts = 0
        if 'count_of_posts' in df.columns:
            total_posts = df['count_of_posts'].sum()
        html_content = html_content.replace('1,343,492 Posts', f'{total_posts:,} Posts')
        
        # Replace the entire allData array - more robust approach
        import re
        
        # First, let's find the exact pattern in the playground template
        # The playground has a very large data array, so we need to be more specific
        
        # Method 1: Try to find and replace the entire allData declaration
        data_start = html_content.find('const allData = [')
        if data_start != -1:
            # Find the matching closing bracket and semicolon
            bracket_count = 0
            data_end = data_start + len('const allData = ')
            in_string = False
            escape_next = False
            
            for i, char in enumerate(html_content[data_end:], data_end):
                if escape_next:
                    escape_next = False
                    continue
                    
                if char == '\\':
                    escape_next = True
                    continue
                    
                if char == '"' and not escape_next:
                    in_string = not in_string
                    continue
                    
                if not in_string:
                    if char == '[':
                        bracket_count += 1
                    elif char == ']':
                        bracket_count -= 1
                        if bracket_count == 0:
                            # Found the end, look for semicolon
                            if i + 1 < len(html_content) and html_content[i + 1] == ';':
                                data_end = i + 2
                                break
            
            if data_end > data_start:
                # Replace the entire data section
                before = html_content[:data_start]
                after = html_content[data_end:]
                html_content = before + f'const allData = {js_data_str};' + after
                st.write("✅ Successfully replaced allData array using bracket matching")
            else:
                st.warning("⚠️ Could not find end of allData array, using regex fallback")
                # Fallback to regex
                data_pattern = r'const allData = \[.*?\];'
                replacement_data = f'const allData = {js_data_str};'
                html_content = re.sub(data_pattern, replacement_data, html_content, flags=re.DOTALL)
        else:
            st.warning("⚠️ Could not find allData declaration in template")
        
        # Replace other variables
        html_content = html_content.replace('const countryDisplay = "France";', f'const countryDisplay = "{country_display}";')
        html_content = html_content.replace('const dateRange = "2025-09-21 to 2025-09-23";', f'const dateRange = "{date_range}";')
        html_content = html_content.replace('const categoryName = "Football Analytics";', f'const categoryName = "{category_name}";')
        
        # Replace footer
        html_content = html_content.replace(
            '🎮 PLAYGROUND VERSION - Powered by X Analytics • Experiment freely!', 
            f'Powered by X Analytics • Generated {datetime.now().strftime("%B %d, %Y at %I:%M %p")}'
        )
        
        
        # Show a snippet of the HTML to check data injection
        html_snippet_start = html_content.find('const allData = ')
        if html_snippet_start != -1:
            html_snippet_end = html_content.find('];', html_snippet_start)
            if html_snippet_end == -1:
                html_snippet_end = html_content.find('};', html_snippet_start)
            if html_snippet_end != -1:
                html_snippet_end += 2
                snippet = html_content[html_snippet_start:html_snippet_end]
                st.code(snippet[:500] + "..." if len(snippet) > 500 else snippet, language="javascript")
            else:
                st.warning("⚠️ Could not find end of allData for preview")
        else:
            st.warning("⚠️ Could not find allData in generated HTML")
        
        st.success("✅ Step 5: HTML dashboard generated successfully using playground template")
        return html_content
    
    def _parse_json_field(self, json_str):
        """Helper method to safely parse JSON fields"""
        if not json_str or json_str == '[]':
            return []
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return []
    
    def _get_fallback_html_template(self):
        """Fallback HTML template if playground file not found"""
        return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Social Media Analytics Dashboard - X Style</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        :root {
            --x-background: #000000;
            --x-text-primary: #FFFFFF;
            --x-text-secondary: #71767B;
            --x-border-color: #2F3336;
            --x-blue: #1D9BF0;
            --x-card-background: #000000;
            --font-family: "Helvetica Neue", Arial, sans-serif;
        }
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: var(--font-family);
        }
        body {
            background: var(--x-background);
            color: var(--x-text-primary);
            min-height: 100vh;
            overflow-x: hidden;
            font-size: 15px;
            line-height: 1.5;
        }
        .header-bar {
            background: var(--x-background);
            padding: 12px 20px;
            display: grid;
            grid-template-columns: 1fr auto 1fr;
            align-items: center;
            border-bottom: 1px solid var(--x-border-color);
            position: sticky;
            top: 0;
            z-index: 1000;
        }
        .x-logo-header svg {
            width: 30px;
            height: 30px;
        }
        .header-center {
            text-align: center;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            grid-column: 2;
        }
        .header-title {
            font-size: 32px;
            font-weight: 800;
            letter-spacing: -0.5px;
            line-height: 1.1;
        }
        .header-subtitle {
            font-size: 13px;
            color: var(--x-text-secondary);
        }
        .header-right {
            display: flex;
            align-items: center;
            gap: 15px;
            grid-column: 3;
            justify-self: end;
        }
        .metric-pill {
            background: var(--x-card-background);
            color: var(--x-text-primary);
            padding: 8px 16px;
            border-radius: 9999px;
            font-size: 14px;
            font-weight: bold;
            border: 1px solid var(--x-border-color);
        }
        .content {
            padding: 40px 20px;
            text-align: center;
        }
        .fallback-message {
            background: var(--x-card-background);
            border: 1px solid var(--x-border-color);
            border-radius: 12px;
            padding: 40px;
            max-width: 600px;
            margin: 0 auto;
        }
    </style>
</head>
<body>
    <div class="header-bar">
        <span class="x-logo-header">
            <svg viewBox="0 0 24 24" fill="currentColor">
                <g><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"></path></g>
            </svg>
        </span>
        <div class="header-center">
            <div class="header-title">Analytics Dashboard</div>
            <div class="header-subtitle">X Insights Lab | Generated Report</div>
        </div>
        <div class="header-right">
            <div class="metric-pill">Analytics Complete</div>
        </div>
    </div>
    
    <div class="content">
        <div class="fallback-message">
            <h2>📊 Analytics Dashboard Generated</h2>
            <p>Your analytics dashboard has been created successfully. The full interactive version would include:</p>
            <ul style="text-align: left; margin: 20px 0;">
                <li>Interactive charts and visualizations</li>
                <li>Category-based navigation</li>
                <li>Media galleries with top content</li>
                <li>Demographic breakdowns</li>
                <li>AI-generated insights</li>
            </ul>
            <p><em>This is a fallback template. For the full experience, ensure the playground template is available.</em></p>
        </div>
    </div>
    
    <script>
        console.log('Analytics Dashboard - Fallback Template');
        // Data would be injected here in the full version
    </script>
</body>
</html>"""

def main():
    st.title("🚀 Automated Analytics Pipeline")
    st.markdown("**5-Step Automated Pipeline:** Table Check → Impressions → Tagging → Analytics → AI Insights → HTML Report")
    
    # Initialize pipeline
    pipeline = AnalyticsPipeline()
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("🛠️ Pipeline Configuration")
        
        # Country selection
        st.markdown("### 🌍 Country Selection")
        st.markdown("""
        **Popular codes:** gb, us, fr, de, es, it, ca, au, jp, br, mx, in, nl, se, no, dk, fi, be, at, ch, pl, cz, hu, ro, gr, pt, ie, lu, sk, si, hr, bg, lt, lv, ee, mt, cy
        """)
        
        country = st.text_input(
            "Enter Country Code (2-letter ISO code)",
            value="gb",
            placeholder="e.g., gb, us, fr, de...",
            help="Enter any 2-letter ISO country code in lowercase. Examples: gb (UK), us (USA), fr (France), de (Germany), etc.",
            max_chars=2
        ).lower()  # Convert to lowercase automatically
        
        # Date configuration
        st.subheader("📅 Date Range")
        date_mode = st.radio(
            "Date Selection",
            options=['rolling', 'specific'],
            format_func=lambda x: "Rolling (Last N Days)" if x == 'rolling' else "Specific Date Range"
        )
        
        if date_mode == 'rolling':
            days_back = st.number_input(
                "Days Back",
                min_value=1,
                max_value=90,
                value=28,
                help="Number of days back from yesterday"
            )
            start_date, end_date = pipeline.calculate_date_range('rolling', days_back=days_back)
        else:
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=date.today() - timedelta(days=28),
                    max_value=date.today() - timedelta(days=1)
                )
            with col2:
                end_date = st.date_input(
                    "End Date", 
                    value=date.today() - timedelta(days=1),
                    max_value=date.today() - timedelta(days=1)
                )
        
        st.write(f"**Analysis Period:** {start_date} to {end_date}")
        
        # Category configuration
        st.subheader("🏷️ Category Settings")
        category_name = st.text_input(
            "Category Name",
            value="SHOES",
            help="Name for your analysis category (used in table names)"
        )
        
        # Template upload
        st.subheader("📤 Template Upload")
        uploaded_file = st.file_uploader(
            "Upload Template CSV",
            type="csv",
            help="CSV with Sub Category, Terms, Handles, Hashtags, Entity ID columns"
        )
    
    # Main content area
    if uploaded_file is not None and category_name:
        # Validate template
        try:
            template_df = pd.read_csv(uploaded_file)
            
            # Basic validation
            required_columns = ['Sub Category', 'Terms', 'Handles', 'Hashtags', 'Entity ID']
            missing_columns = [col for col in required_columns if col not in template_df.columns]
            
            if missing_columns:
                st.error(f"❌ Missing required columns: {', '.join(missing_columns)}")
                return
            
            st.success(f"✅ Template validated! Found {len(template_df)} brands.")
            
            # Show template preview
            with st.expander("📋 Template Preview"):
                st.dataframe(template_df.head())
            
            # Pipeline execution button
            if st.button("🚀 Run Complete Pipeline", type="primary", use_container_width=True):
                if not st.session_state.processing:
                    st.session_state.processing = True
                    
                    try:
                        # Initialize clients
                        if not pipeline.initialize_clients():
                            st.session_state.processing = False
                            return
                        
                        # Create progress tracking
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Step 1: Create impressions table (with existence check)
                        status_text.text("Step 1/5: Checking/Creating impressions table...")
                        progress_bar.progress(0.1)
                        
                        success, impressions_table = pipeline.step1_create_impressions_table(
                            country, start_date, end_date
                        )
                        if not success:
                            st.session_state.processing = False
                            return
                        
                        progress_bar.progress(0.2)
                        
                        # Step 2: Upload template and create tagged table
                        status_text.text("Step 2/5: Uploading template and tagging posts...")
                        progress_bar.progress(0.3)
                        
                        success, tagged_table = pipeline.step2_upload_and_tag(
                            template_df, country, start_date, end_date, category_name, impressions_table
                        )
                        if not success:
                            st.session_state.processing = False
                            return
                        
                        progress_bar.progress(0.5)
                        
                        # Step 3: Run analytics query
                        status_text.text("Step 3/5: Running comprehensive analytics...")
                        progress_bar.progress(0.6)
                        
                        success, analytics_df = pipeline.step3_run_analytics(tagged_table)
                        if not success:
                            st.session_state.processing = False
                            return
                        
                        progress_bar.progress(0.7)
                        
                        # Step 4: Generate AI summaries
                        status_text.text("Step 4/5: Generating AI insights...")
                        progress_bar.progress(0.8)
                        
                        analytics_df = pipeline.step4_generate_ai_summaries(analytics_df)
                        
                        progress_bar.progress(0.9)
                        
                        # Step 5: Generate HTML report
                        status_text.text("Step 5/5: Creating HTML dashboard...")
                        html_report = pipeline.step5_generate_html_report(analytics_df, category_name)
                        
                        progress_bar.progress(1.0)
                        status_text.text("✅ Pipeline completed successfully!")
                        
                        # Store results
                        st.session_state.results = {
                            'analytics_df': analytics_df,
                            'html_report': html_report,
                            'category_name': category_name,
                            'country': country,
                            'start_date': start_date,
                            'end_date': end_date
                        }
                        
                        st.session_state.processing = False
                        st.success("🎉 **Pipeline completed successfully!** Results are ready for download.")
                        
                    except Exception as e:
                        st.session_state.processing = False
                        st.error(f"❌ Pipeline failed: {str(e)}")
                        logger.error(f"Pipeline error: {str(e)}")
            
        except Exception as e:
            st.error(f"❌ Error reading template: {str(e)}")
    
    # Clear results button
    if st.session_state.results is not None:
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🔄 Clear Results", help="Clear cached results to run fresh pipeline"):
                st.session_state.results = None
                st.rerun()
        with col2:
            st.info("💡 **Updated SQL Query Active** - Run new pipeline to see Authors & Trends data")
        with col3:
            if st.button("🔍 Debug Data", help="Show raw data columns for debugging"):
                st.write("**Available Columns:**")
                st.write(list(st.session_state.results['analytics_df'].columns))
                st.write("**Sample Data:**")
                st.dataframe(st.session_state.results['analytics_df'].head(1))
    
    # Results section
    if st.session_state.results is not None:
        results = st.session_state.results
        analytics_df = results['analytics_df']
        
        st.header("📊 Analytics Dashboard")
        
        # Category selector
        if len(analytics_df) > 1:
            selected_category = st.selectbox(
                "Select Category to Analyze:",
                options=analytics_df['tags'].tolist(),
                key="category_selector"
            )
            category_data = analytics_df[analytics_df['tags'] == selected_category].iloc[0]
        else:
            category_data = analytics_df.iloc[0]
            selected_category = category_data['tags']
        
        # Dashboard tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Overview", "📈 Metrics", "👥 Audience", "📊 Trends", "🎬 Media"])
        
        with tab1:
            display_overview_tab(analytics_df, results)
        
        with tab2:
            display_metrics_tab(category_data)
        
        with tab3:
            display_audience_tab(category_data)
        
        with tab4:
            display_trends_tab(category_data)
        
        with tab5:
            display_media_tab(category_data)
        
        # Download section
        st.header("📥 Download Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV download
            csv = analytics_df.to_csv(index=False)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="📊 Download CSV Data",
                data=csv,
                file_name=f"analytics_{results['category_name']}_{results['country']}_{timestamp}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # HTML download
            st.download_button(
                label="🌐 Download HTML Dashboard",
                data=results['html_report'],
                file_name=f"dashboard_{results['category_name']}_{results['country']}_{timestamp}.html",
                mime="text/html",
                use_container_width=True
            )

def display_overview_tab(analytics_df, results):
    """Display overview tab with summary metrics and data preview"""
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_impressions = analytics_df['impressions'].sum()
    total_posts = analytics_df['count_of_posts'].sum()
    total_engagement = analytics_df['reply'].sum() + analytics_df['favorite'].sum() + analytics_df['retweet'].sum()
    avg_engagement_rate = (total_engagement / total_posts * 100) if total_posts > 0 else 0
    
    with col1:
        st.metric("Total Impressions", f"{total_impressions:,}")
    with col2:
        st.metric("Total Posts", f"{total_posts:,}")
    with col3:
        st.metric("Total Engagement", f"{total_engagement:,}")
    with col4:
        st.metric("Avg Engagement Rate", f"{avg_engagement_rate:.2f}%")
    
    # Results preview
    with st.expander("📋 Analytics Results Preview", expanded=True):
        st.dataframe(
            analytics_df[['tags', 'count_of_posts', 'impressions', 'reply', 'favorite', 'retweet', 'ai_summary']].head(10),
            use_container_width=True
        )
    
    st.info("💡 **Pipeline Summary:**\n"
            f"- **Country:** {results['country'].upper()}\n"
            f"- **Date Range:** {results['start_date']} to {results['end_date']}\n"
            f"- **Category:** {results['category_name']}\n"
            f"- **Brands Analyzed:** {len(analytics_df)}\n"
            f"- **Total Posts:** {total_posts:,}\n"
            f"- **Total Impressions:** {total_impressions:,}")

def display_metrics_tab(category_data):
    """Display metrics tab with key performance indicators"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 Total Posts",
            value=f"{category_data['count_of_posts']:,}"
        )
    
    with col2:
        st.metric(
            label="👁️ Impressions",
            value=f"{category_data['impressions']:,}"
        )
    
    with col3:
        engagement = category_data['reply'] + category_data['favorite'] + category_data['retweet']
        st.metric(
            label="💬 Total Engagement",
            value=f"{engagement:,}"
        )
    
    with col4:
        engagement_rate = (engagement / category_data['count_of_posts'] * 100) if category_data['count_of_posts'] > 0 else 0
        st.metric(
            label="📈 Engagement Rate",
            value=f"{engagement_rate:.2f}%"
        )
    
    # Detailed metrics
    st.subheader("📋 Detailed Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("💬 Replies", f"{category_data['reply']:,}")
        st.metric("🔄 Retweets", f"{category_data['retweet']:,}")
    
    with col2:
        st.metric("❤️ Favorites", f"{category_data['favorite']:,}")
        if 'video_views' in category_data and pd.notna(category_data['video_views']):
            st.metric("📹 Video Views", f"{category_data['video_views']:,}")
    
    with col3:
        avg_impressions = category_data['impressions'] / category_data['count_of_posts'] if category_data['count_of_posts'] > 0 else 0
        st.metric("📊 Avg Impressions/Post", f"{avg_impressions:,.0f}")

def display_audience_tab(category_data):
    """Display audience tab with demographics and author information"""
    # Debug info
    with st.expander("🔍 Debug: Available Data Fields", expanded=False):
        st.write("**Available fields in category_data:**")
        available_fields = [col for col in category_data.index if pd.notna(category_data[col])]
        st.write(available_fields)
        if 'no_of_authors' in category_data.index:
            st.write(f"**no_of_authors value:** {category_data['no_of_authors']}")
        if 'age_split' in category_data.index:
            st.write(f"**age_split value:** {category_data['age_split']}")
        if 'gender_split' in category_data.index:
            st.write(f"**gender_split value:** {category_data['gender_split']}")
        if 'vertical_of_interest' in category_data.index:
            st.write(f"**vertical_of_interest value:** {category_data['vertical_of_interest']}")
    
    col1, col2, col3 = st.columns([1, 0.8, 1])
    
    # Age Demographics
    with col1:
        st.subheader("📊 Age Demographics")
        if 'age_split' in category_data.index and pd.notna(category_data['age_split']):
            try:
                # Parse age_split format: "18-24 25.5,25-34 35.2,35-44 20.1"
                age_data = {}
                age_entries = category_data['age_split'].split(',')
                for entry in age_entries:
                    parts = entry.strip().split(' ')
                    if len(parts) >= 2:
                        age_group = parts[0]
                        percentage = float(parts[1])
                        age_data[age_group] = percentage
                
                if age_data:
                    # Create age distribution chart
                    chart_data = pd.DataFrame({
                        'Age Group': list(age_data.keys()),
                        'Percentage': list(age_data.values())
                    })
                    st.bar_chart(chart_data.set_index('Age Group'))
                else:
                    st.info("No age demographics data available")
            except (ValueError, Exception) as e:
                st.info(f"Age demographics data format not supported: {str(e)}")
        else:
            st.info("No age demographics data available")
    
    # Gender Distribution
    with col2:
        st.subheader("⚧️ Gender Distribution")
        if 'gender_split' in category_data.index and pd.notna(category_data['gender_split']):
            try:
                # Parse gender_split format: "MALE 65.5,FEMALE 34.5"
                gender_data = {}
                gender_entries = category_data['gender_split'].split(',')
                for entry in gender_entries:
                    parts = entry.strip().split(' ')
                    if len(parts) >= 2:
                        gender = parts[0]
                        percentage = float(parts[1])
                        gender_data[gender] = percentage
                
                if gender_data:
                    # Create pie chart for gender distribution
                    df_gender = pd.DataFrame(list(gender_data.items()), columns=['Gender', 'Percentage'])
                    fig = px.pie(df_gender, values='Percentage', names='Gender', 
                               color_discrete_sequence=['#00FFFF', '#FF00FF'])
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(showlegend=True, height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No gender distribution data available")
            except (ValueError, Exception) as e:
                st.info(f"Gender distribution data format not supported: {str(e)}")
        else:
            st.info("No gender distribution data available")
    
    # Total Authors and Audience Interests
    with col3:
        # Total Authors
        st.subheader("👥 Total Authors")
        if 'no_of_authors' in category_data.index and pd.notna(category_data['no_of_authors']):
            st.metric("", f"{int(category_data['no_of_authors']):,}")
        else:
            st.metric("", "N/A")
        
        st.markdown("---")
        
        # Audience Interests
        st.subheader("🎯 Audience Interests")
        if 'vertical_of_interest' in category_data.index and pd.notna(category_data['vertical_of_interest']):
            interests = category_data['vertical_of_interest'].split(',')
            for interest in interests:
                st.write(f"• {interest.strip()}")
        else:
            st.info("No audience interests data available")

def display_trends_tab(category_data):
    """Display trends tab with daily activity chart"""
    # Debug info
    with st.expander("🔍 Debug: Daily Data", expanded=False):
        if 'daily_data_json' in category_data.index:
            st.write(f"**daily_data_json exists:** {pd.notna(category_data['daily_data_json'])}")
            if pd.notna(category_data['daily_data_json']):
                st.write(f"**daily_data_json type:** {type(category_data['daily_data_json'])}")
                st.write(f"**daily_data_json value:** {str(category_data['daily_data_json'])[:500]}...")
                # Try to parse it
                try:
                    parsed_data = json.loads(category_data['daily_data_json'])
                    st.write(f"**JSON parsing successful! Found {len(parsed_data)} days**")
                    st.write(f"**First day sample:** {parsed_data[0] if parsed_data else 'No data'}")
                except Exception as e:
                    st.write(f"**JSON parsing failed:** {str(e)}")
            else:
                st.write("**daily_data_json is null/empty**")
        else:
            st.write("**daily_data_json field not found**")
    
    st.subheader("📈 Daily Activity Trends")
    
    if 'daily_data_json' in category_data.index and pd.notna(category_data['daily_data_json']):
        try:
            daily_data = json.loads(category_data['daily_data_json'])
            
            if daily_data and len(daily_data) > 0:
                # Prepare data for chart
                dates = []
                post_counts = []
                top_posts = []
                
                for day_data in daily_data:
                    # Handle different possible field names
                    date_field = day_data.get('date') or day_data.get('post_date')
                    count_field = day_data.get('post_count') or day_data.get('no_of_posts')
                    top_post_field = day_data.get('top_post_day') or day_data.get('top_post')
                    
                    if date_field and count_field is not None:
                        dates.append(date_field)
                        post_counts.append(int(count_field))
                        top_posts.append(top_post_field or {})
                
                if dates and post_counts:
                    # Create DataFrame
                    df_trends = pd.DataFrame({
                        'Date': pd.to_datetime(dates),
                        'Posts': post_counts
                    })
                    
                    # Display line chart
                    st.line_chart(df_trends.set_index('Date'))
                    
                    # Display clickable data points info
                    st.subheader("📊 Daily Breakdown")
                    
                    for i, (date, count, top_post) in enumerate(zip(dates, post_counts, top_posts)):
                        with st.expander(f"📅 {date} - {count:,} posts"):
                            if top_post and isinstance(top_post, dict) and 'tweetId' in top_post:
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    post_text = top_post.get('text', 'N/A')
                                    if len(post_text) > 100:
                                        post_text = post_text[:100] + "..."
                                    st.write(f"**Top Post:** {post_text}")
                                    impressions = top_post.get('Impressions') or top_post.get('impressions', 'N/A')
                                    if impressions != 'N/A':
                                        st.write(f"**Impressions:** {impressions:,}")
                                    else:
                                        st.write(f"**Impressions:** {impressions}")
                                with col2:
                                    tweet_url = f"https://x.com/i/web/status/{top_post['tweetId']}"
                                    st.link_button("🔗 View on X", tweet_url)
                            else:
                                st.info("No top post data available for this day")
                else:
                    st.warning("Could not parse daily data - missing date or count fields")
            else:
                st.info("No daily trends data available")
        except (json.JSONDecodeError, Exception) as e:
            st.error(f"Error parsing daily trends data: {str(e)}")
    else:
        st.info("No daily trends data available")

def display_media_tab(category_data):
    """Display media tab with top content"""
    # Top Posts
    st.subheader("🔥 Top Posts")
    if 'top_posts' in category_data and pd.notna(category_data['top_posts']):
        try:
            top_posts = json.loads(category_data['top_posts'])
            if top_posts:
                for i, post in enumerate(top_posts[:5], 1):
                    with st.expander(f"Post #{i} - {post.get('impressions', 0):,} impressions"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(post.get('text', 'N/A'))
                            st.caption(f"👁️ {post.get('impressions', 0):,} impressions")
                        with col2:
                            if 'tweetId' in post:
                                tweet_url = f"https://x.com/i/web/status/{post['tweetId']}"
                                st.link_button("🔗 View", tweet_url)
            else:
                st.info("No top posts data available")
        except (json.JSONDecodeError, Exception):
            st.info("Top posts data format not supported")
    else:
        st.info("No top posts data available")
    
    # Top Photos
    st.subheader("📸 Top Photos")
    if 'top_photos' in category_data and pd.notna(category_data['top_photos']):
        try:
            top_photos = json.loads(category_data['top_photos'])
            if top_photos:
                for i, photo in enumerate(top_photos[:3], 1):
                    with st.expander(f"Photo #{i} - {photo.get('impressions', 0):,} impressions"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(photo.get('text', 'N/A'))
                            st.caption(f"👁️ {photo.get('impressions', 0):,} impressions")
                        with col2:
                            if 'tweetId' in photo:
                                tweet_url = f"https://x.com/i/web/status/{photo['tweetId']}"
                                st.link_button("🔗 View", tweet_url)
            else:
                st.info("No top photos data available")
        except (json.JSONDecodeError, Exception):
            st.info("Top photos data format not supported")
    else:
        st.info("No top photos data available")
    
    # Top Videos
    st.subheader("📹 Top Videos")
    if 'top_videos' in category_data and pd.notna(category_data['top_videos']):
        try:
            top_videos = json.loads(category_data['top_videos'])
            if top_videos:
                for i, video in enumerate(top_videos[:3], 1):
                    with st.expander(f"Video #{i} - {video.get('impressions', 0):,} impressions"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(video.get('text', 'N/A'))
                            metrics_col1, metrics_col2 = st.columns(2)
                            with metrics_col1:
                                st.caption(f"👁️ {video.get('impressions', 0):,} impressions")
                            with metrics_col2:
                                st.caption(f"📹 {video.get('video_views', 0):,} video views")
                        with col2:
                            if 'tweetId' in video:
                                tweet_url = f"https://x.com/i/web/status/{video['tweetId']}"
                                st.link_button("🔗 View", tweet_url)
            else:
                st.info("No top videos data available")
        except (json.JSONDecodeError, Exception):
            st.info("Top videos data format not supported")
    else:
        st.info("No top videos data available")
    
    # Top GIFs
    st.subheader("🎭 Top GIFs")
    if 'top_gifs' in category_data and pd.notna(category_data['top_gifs']):
        try:
            top_gifs = json.loads(category_data['top_gifs'])
            if top_gifs:
                for i, gif in enumerate(top_gifs[:3], 1):
                    with st.expander(f"GIF #{i} - {gif.get('impressions', 0):,} impressions"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(gif.get('text', 'N/A'))
                            st.caption(f"👁️ {gif.get('impressions', 0):,} impressions")
                        with col2:
                            if 'tweetId' in gif:
                                tweet_url = f"https://x.com/i/web/status/{gif['tweetId']}"
                                st.link_button("🔗 View", tweet_url)
            else:
                st.info("No top GIFs data available")
        except (json.JSONDecodeError, Exception):
            st.info("Top GIFs data format not supported")
    else:
        st.info("No top GIFs data available")
    
    # Top Engagement Posts
    st.subheader("🚀 Top Engagement Posts")
    if 'top_engagement_posts' in category_data and pd.notna(category_data['top_engagement_posts']):
        try:
            top_engagement = json.loads(category_data['top_engagement_posts'])
            if top_engagement:
                for i, post in enumerate(top_engagement[:5], 1):
                    total_engagement = (post.get('reply', 0) + post.get('favorite', 0) + 
                                     post.get('retweet', 0))
                    with st.expander(f"Post #{i} - {total_engagement:,} total engagement"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(post.get('text', 'N/A'))
                            st.caption(f"🚀 {total_engagement:,} total engagement")
                        with col2:
                            if 'tweetId' in post:
                                tweet_url = f"https://x.com/i/web/status/{post['tweetId']}"
                                st.link_button("🔗 View", tweet_url)
            else:
                st.info("No top engagement posts data available")
        except (json.JSONDecodeError, Exception):
            st.info("Top engagement posts data format not supported")
    else:
        st.info("No top engagement posts data available")

    def process_csv_content(self, csv_content):
        """Process CSV content and return results DataFrame"""
        try:
            # Save CSV content to temporary file
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                temp_file.write(csv_content)
                temp_file_path = temp_file.name
            
            # Process the file
            results_df = self.step3_run_analytics(temp_file_path)
            
            # Clean up temp file
            import os
            os.unlink(temp_file_path)
            
            return results_df
            
        except Exception as e:
            print(f"Error processing CSV content: {e}")
            return None
    
    def generate_sample_html_report(self):
        """Generate a sample HTML report"""
        try:
            # Use the existing playground HTML as template
            with open('playground_analytics_dashboard.html', 'r') as f:
                html_content = f.read()
            
            # Add sample data
            sample_data = {
                'Google Pixel': {
                    'name': 'Google Pixel',
                    'impressions': 45000,
                    'posts': 1500,
                    'no_of_authors': 150,
                    'engagement': 12000
                },
                'Samsung Galaxy': {
                    'name': 'Samsung Galaxy', 
                    'impressions': 67000,
                    'posts': 2300,
                    'no_of_authors': 230,
                    'engagement': 18000
                },
                'iPhone 17': {
                    'name': 'iPhone 17',
                    'impressions': 89000,
                    'posts': 3200,
                    'no_of_authors': 320,
                    'engagement': 25000
                }
            }
            
            # Replace placeholder data in HTML
            html_content = html_content.replace(
                'const brandData = {};',
                f'const brandData = {json.dumps(sample_data)};'
            )
            
            return html_content
            
        except Exception as e:
            print(f"Error generating HTML report: {e}")
            return "<html><body><h1>Error generating report</h1></body></html>"

if __name__ == "__main__":
    main()
