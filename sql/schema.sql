CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE d_company (
        id_company INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT,
        sector TEXT
    );
CREATE TABLE IF NOT EXISTS "d_country" (
"id_country" INTEGER,
  "iso2" TEXT,
  "country_name" TEXT,
  "region" TEXT,
  "currency" TEXT
);
CREATE TABLE IF NOT EXISTS "d_skill" (
"id_skill" INTEGER,
  "skill_group" TEXT,
  "tech_label" TEXT
);
CREATE TABLE IF NOT EXISTS "d_source" (
"id_source" INTEGER,
  "source_name" TEXT
);
CREATE TABLE IF NOT EXISTS "d_date" (
"date_key" TEXT,
  "day" INTEGER,
  "month" INTEGER,
  "quarter" INTEGER,
  "year" INTEGER,
  "day_week" INTEGER
);
CREATE TABLE IF NOT EXISTS "f_job_offers" (
"id_country" INTEGER,
  "id_skill" INTEGER,
  "id_source" INTEGER,
  "id_company" INTEGER,
  "date_key" TEXT,
  "title" TEXT,
  "location" TEXT,
  "salary_min" REAL,
  "salary_max" REAL,
  "salary_avg" REAL
);
CREATE TABLE IF NOT EXISTS "f_github_trends" (
"id_skill" INTEGER,
  "id_source" INTEGER,
  "date_key" TEXT,
  "repo_name" TEXT,
  "stars" INTEGER,
  "forks" INTEGER,
  "popularity_score" REAL
);
CREATE TABLE IF NOT EXISTS "f_search_trends" (
"id_skill" INTEGER,
  "id_source" INTEGER,
  "date_key" TEXT,
  "interest_value" INTEGER
);
CREATE TABLE IF NOT EXISTS "f_survey_responses" (
"id_country" INTEGER,
  "id_source" INTEGER,
  "salary" REAL,
  "years_experience" TEXT,
  "dev_type" TEXT,
  "languages_used" TEXT
);
CREATE INDEX idx_job_country ON f_job_offers(id_country);
CREATE INDEX idx_job_skill ON f_job_offers(id_skill);
CREATE INDEX idx_job_date ON f_job_offers(date_key);
CREATE INDEX idx_github_skill ON f_github_trends(id_skill);
CREATE INDEX idx_trends_skill ON f_search_trends(id_skill);
CREATE INDEX idx_survey_country ON f_survey_responses(id_country);
