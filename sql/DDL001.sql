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
  OWNER TO postgres;

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
  OWNER TO postgres;