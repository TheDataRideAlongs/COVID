# Project Domino

## Scaling COVID public behavior change and anti-misinformation

* [Community Slack channel: #COVID](https://thedataridealongs.slack.com/) via an [open invite link](https://join.slack.com/t/thedataridealongs/shared_invite/zt-d06nq64h-P1_3sENXG4Gg0MjWh1jPEw)

* [Meetings: Google calendar](https://calendar.google.com/calendar?cid=Z3JhcGhpc3RyeS5jb21fdTQ3bmQ3YTdiZzB0aTJtaW9kYTJybGx2cTBAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ)

* Project Tackers: [Core](https://github.com/TheDataRideAlongs/ProjectDomino/projects/1), [Interventions](https://github.com/TheDataRideAlongs/ProjectDomino/projects/2), and [Issues](https://github.com/TheDataRideAlongs/ProjectDomino/issues)

* Tutorials: [available Jupyter-based notebook analyses](tutorials.md)

* Infra - ask for account access in Slack
  * [Private repository](https://github.com/graphistry/ProjectDomino-internal)
  * Observability - [Grafana/prometheus](http://13.68.225.97:10007/d/n__-I5jZk/neo4j-4-dashboard?orgId=1) and Jupyter notebooks
  * DB - Neo4j
  * Orchestration - Prefect
  * Key sharing - Keybase team
  * CI - Github Actions
  * [Labeling funnel & feeds](https://docs.google.com/spreadsheets/d/1FqLYCJqPcxkVYKOn4uQzl0A_7MV4zkYOF94BAITvta0), [proposed extended schema](https://docs.google.com/spreadsheets/d/1VM8JfV23wsLGJ3fl3mGW3Wi3n9Z9-D1NWUfBc4s0M54), and [current knowledge graph schema](https://www.lucidchart.com/documents/edit/674bc883-d01a-449a-8666-5cfc841c54bb/0_0?shared=true)

* **Graph4good contributors:** We're excited to work with you! Check out the subprojects below we are actively seeking help on, look at some of the Github Issues, and ping on Slack for which you're curious about tackling and others not here. We can then share our current thoughts and tips for getting started. Most will be useful as pure Python [Google Collab notebook](https://colab.research.google.com) proveouts and local Neo4j Docker + Python proveouts: You can move quickly, and we can more easily integrate into our automation pipelines.

**One of the most important steps in stopping the COVID-19 pandemic is influencing mass behavior change for citizens to take appropriate, swift action on mitigating infection and human-to-human contact.** Government officials at all levels have advocated misinformed practices such as dining out or participating in outdoor gatherings that have contributed to amplifying the curve rather than flattening it. At time of writing, the result of poor crisis emergency risk communication has led to over 32.9M US citizens testing positive, 2-20X more are likely untested, and over 584K deaths. The need to influence appropriate behavior and mitigation actions are extreme: The US has shot up from untouched to become the 6th most infected nation. 

**Project Domino accelerates research on developing capabilities for information hygiene at the mass scale necessary for the current national disaster and for future ones.** We develop and enable the use of 3 key data capabilities for modern social discourse: 
* Detecting misinformation campaigns
* Identifying at-risk behavior groups and viable local behavior change influencers
* Automating high-precision interventions

## Data

We are collecting, analyzing, and sharing data around:

* COVID Twitter: Who is saying what and when, including around URLs
* Feeds: Correlations against labeled data such as bots, fact checks, and indicators of digital crime  
* Scores: Bots, misinformation, crime, likely location, and more

While we cannot publish the raw data due due to compliance restrictions from our data providers, we are happy to support individual projects, such as for analyzing and predicting real-world compliance of health policies, and identifying bad actors. Please jump into the Slack or contact a project leader on LinkedIn and we'll get you going.

## The interventions

We are working with ethics groups to identify safe interventions along the following lines:

* **Targeting of specific underserved issues**: Primary COVID public health issues such as unsafe social behavior, unsafe medicine, unsafe science, dangerous government policy influence, and adjacent issues such as fake charities, phishing,  malware, and hate group propaganda

* **Help top social platforms harden themselves**: Trust and safety teams at top social networks need to be able to warn users about misinformation, de-trend it, and potentially take it down before it has served its purpose. The status quo is handling incidents months after the fact. We will provide real-time alert feeds and scoring APIs to help take action during the critical minutes before misinformation gains significant reach.

* **Enable top analysts to investigate coordinated activity**: A minority of groups cause the bulk of the misinformation that gets shared. We are building a high-scale analyst environment featuring technology such as GPU-accelerated visual graph analytics and high-memory notebook servers.

* **Help leaders clean up their community**: Identify and invite community leaders of at-risk groups to use our tools to detect trending misinformation and sift it out from their regular community content.

* **Alert individuals as they are being manipulated**: For manipulated conversations where we have clear intelligence, we are exploring an alert bot that will post the misinformation report directly on the thread, or enable community participants or project partners to do so.

* **Enable other platforms**: We expect a growing number of initiatives to benefit from our intelligence and automation capabilities.

## The technologies

* Twitter firehose monitor
* Data integration pipeline for sources of known scams, fraud, lies, bots, propaganda, extremism, and other misinformation sources
* Misinformation knowledge graph connecting accounts, posts, reports, and models
* Automated GPU / graph / machine learning pipeline: general classification (bot, community, ...) and targeted (clinical disinformation, ...)
* Automated alerting & reporting pipeline
* Interactive visual analytics environment for data scientists and analysts: GPU, graph, notebooks, ML, ...
* Intervention bots

## How to help

We are actively seeking several forms of support:

* **Volunteers**: Most immediate priority is on data engineering and advisors on marketing/public health
  * **Data engineers: Orchestration (Airflow, Prefect.io, Nifi, ...), streaming (Kafka, ...),  graph (Neo4j, cuGraph), GPU (RAPIDS), ML (NLP libs), and databases**
  * **Analysts: OSINT, threat intel, campaign tracking, ...**
  * **Data scientists: especially around graph, misinformation, neural networks, NLP, with backgrounds such as security, fraud,  misinformation, marketing**
  * Developers & designers: intelligence integrations, website for search & reports, automations, intervention bots, API
  * Marketing: Strategy & implementation
  * Public health and communications: Especially around safe and effective intervention design
  * Legal: Risk & legal analysis for various interventions

* **APIs and Data**: 
  * Feeds & enriching APIs: Lists and intel on URLs, domains, keywords, emails, topics, blockchain accounts, social media accounts & content, clinical trials, esp. if tunable on topic
  * Scoring: Libraries and APIs around social networks (initially Twitter) structure & content, websites, and news: bot scores, fingerprinting, topic extraction & classification
  * Crawling tech: Social networks and web

* **Software Licenses**:
  * **SaaS, OSS, Docker preferred**
  * Project management
  * Analytics
  * Automation

* **Hardware**: Anything you can provide along the lines of:
  * 1 x Database server (CPU): 32+ cores, Ubuntu 18, 64GB+ RAM, ideally backups
  * 1 x Primary analytics server (GPU) - 32+ CPU cores, 128GB+ CPU RAM, 2-8 GPUs, 64GB+ disk, 2-10TB attached SSD
    * GPUs: Nvidia Pascal or later, 12GB minimum, with 32GB **strongly** preferred (Ex: 32GB Volta)
  * 2 x Secondary / developer servers (GPU) - 8+ CPU cores, 64GB+ CPU RAM, 2-4 GPUs, 64GB+ disk, 1TB attached SSD
    * GPUs: Nvidia Pascal or later, 12GB minimum
  * 3 x Analyst stations (GPU) - 8+ CPU cores, 64GB+ CPU RAM, 1 TB attached SSD, 1-2 GPUs
    * GPUs: Nvidia Pascal or later, 12GB minimum, with 32GB **strongly** preferred (Ex: 32GB Volta)
  * For each, we can work together to setup Ubuntu with remote SSH admin, Nvidia drivers, & (Nvidia-enabled) Docker

* **Sponsors**: Near-term funding until the project finds a more sustainable path is welcome!

* **Subprojects**: We are focusing near-term on core data pipeline and simple analyses while building up to the discourse-graph-level ones

  * Firehose pipeline: 
     * Cleaning up Twitter -> Neo4j/Parquet data conversions
     * Prefect.io/Airflow Firehose tasks: usertimeline, topic search, ... => Parquet/Neo4j
     * Switching from Twarc to Twint for higher-volume ingest
     * Fast large graph (100M row) export of cypher->neo4j->parquet
  * Orchestration tasks:
     * Python entity extractors: tweet -> text, URLs, bitcoin address, topics, ...
     * Python enrichment tasks: external APIs (factcheck, crypto, ...) and lightweight NLP algs (sentiment) and feed back to neo4j
     * Python data feed collectors: scripts that download feeds (factcheck, phishing, ...) and feed back to neo4j
  * Intervention campaigns: 
     * **Priority**: Untrialed drug misinformation - modeling, detection, analysis, report, & alert
     * Scams: Check URLs & blockchain addresses for known badness
     * Mapping bots & misinformers
  * Automation: Prototypes of
     * Alert feed + scoring API: Misinfo intel for trust & safety teams that is powered by Neo4j and/or Prefect.io/Airflow
     * Anti-misinfo chatbot: Upon a misinfo detection, respond to a Twitter thread with a misinfo report URL + WHO safety guide
     * Community leader tools: Personalized alerting for community leaders to detect & respond to misinfo in their close social networks
  

## Contact

Please contact [Leo Meyerovich, CEO @ Graphistry](https://www.linkedin.com/in/leo-meyerovich-09649219) and [Sean Griffin, CEO @ DisasterTech](https://www.linkedin.com/in/seanmichaelgriffin/) for support and information

[Community Slack channel: #COVID](https://thedataridealongs.slack.com/) via an [open invite link](https://join.slack.com/t/thedataridealongs/shared_invite/zt-d06nq64h-P1_3sENXG4Gg0MjWh1jPEw)
