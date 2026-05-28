# Kestra OSS Use Case Demos

A collection of step-by-step guides for learning about workflow orchestration with [Kestra](https://kestra.io). Each use case is self-contained with its own Docker Compose stack, sample data, and a tutorial that walks you from zero to a working solution. No prior Kestra experience required to follow along.

## Use Cases

| # | Use Case | Description |
|---|---|---|
| 01 | [Real-Time Sentiment Analysis](01_sentiment_analysis/) | Build an event-driven data pipeline that consumes customer reviews from Kafka, uses an LLM to extract sentiment and key topics, stores enriched vector embeddings in MongoDB, and fires Slack alerts for negative feedback from premium customers. |
