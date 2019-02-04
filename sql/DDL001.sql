
-- Database: DATA_AGGR

-- DROP DATABASE "DATA_AGGR";

CREATE DATABASE "DATA_AGGR"
    WITH 
    OWNER = developer
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

(Switch to database  DATA_AGGR);

-- Table: public.base_aggr

-- DROP TABLE public.base_aggr;

CREATE TABLE public.base_aggr
(
  portfolio_id text NOT NULL,
  scenario_id integer NOT NULL,
  sum_pnl double precision,
  CONSTRAINT "BASE_AGGR_PK" PRIMARY KEY (portfolio_id, scenario_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.base_aggr
  OWNER TO developer;

---------------------------------

-- Table: public.whatif_aggr

-- DROP TABLE public.whatif_aggr;

CREATE TABLE public.whatif_aggr
(
  portfolio_id text NOT NULL,
  scenario_id integer NOT NULL,
  sum_pnl double precision,
  CONSTRAINT "WHATIF_aggr_PK" PRIMARY KEY (portfolio_id, scenario_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.whatif_aggr
  OWNER TO developer;


----------------------------
CREATE SEQUENCE drilldown_request_seq START 101;

-- DROP TABLE drilldown_request_table;
CREATE TABLE public.drilldown_request_table
(
  drilldown_request_id integer NOT NULL,
  filter_condition text NOT NULL,
  drilldown_dim text NOT NULL,
  processed boolean DEFAULT FALSE,
  CONSTRAINT "drilldown_request_PK" PRIMARY KEY (drilldown_request_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.drilldown_request_table
  OWNER TO developer;
  --------------------

-- DROP TABLE drilldown_table
CREATE TABLE public.drilldown_table
(
  drilldown_request_id integer NOT NULL,
  dim_1      text NULL,
  scenario_id integer NOT NULL,
  dim_1_pnl   double precision,
  CONSTRAINT "drilldown_table_PK" PRIMARY KEY (drilldown_request_id, dim_1, scenario_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.drilldown_table
  OWNER TO developer;



----------------------------
--- I threw in the insert statement. Run it only if you think you need it.
--------------------
DELETE FROM drilldown_request_table;

INSERT INTO drilldown_request_table 
(drilldown_request_id, filter_condition, drilldown_dim)
values (nextval('drilldown_request_seq'), 'portFolioId=''PORTF_003''','prodId' );

commmit;

---------------------


CREATE ROLE dev
  SUPERUSER INHERIT CREATEDB CREATEROLE REPLICATION VALID UNTIL '2024-02-24 00:00:00';


CREATE ROLE developer LOGIN
  ENCRYPTED PASSWORD 'md50e849095ad8db45384a9cdd28d7d0e20'
  SUPERUSER INHERIT CREATEDB CREATEROLE REPLICATION VALID UNTIL '2025-02-22 00:00:00';
GRANT dev TO developer;




