---
name: Tracksuit
title: Tracksuit Fivetran connector
description: Step-by-step instructions on how to connect Tracksuit with your destination using Fivetran connectors.
hidden: false
---

# Tracksuit {% typeBadge connector="tracksuit" /%} {% availabilityBadge connector="tracksuit" /%}

Tracksuit is a Brand Tracker. This connector provides the ability to sync the funnel metrics for any of your tracked brands.

------------------

## Features

{% featureTable connector="singlestore" %}
Capture Deletes: All tables and fields
Column hashing:
API Configurable:
Private networking: AWS Private Link, Azure Private Link, GCP Private Service Connect
{% /featureTable %}

------------------

## Setup guide

Follow our [step-by-step Tracksuit setup guide](/docs/setup-guide.md) to connect Tracksuit with your destination using Fivetran connectors.

------------------
## Sync overview

Once Fivetran is connected to your Tracksuit database, the connector fetches an initial
consistent snapshot of all of your funnel metrics. Once the initial sync is complete this
connector will update the new funnel metrics as surveys are conducted. Tracksuit sync's the survey responses
on the 7th of each month, because of this we recommend using a long sync frequency for the connector.

------------------

## Schema information

Fivetran replicates the funnel metric database with one exception, we add a unique ID to ensure data does not get replicated.

### Fivetran-generated columns

Fivetran adds the following columns to table in your destination:

- `_fivetran_deleted` (BOOLEAN) marks deleted rows in the source database.
- `_fivetran_synced` (UTC TIMESTAMP) indicates when Fivetran last successfully synced the row.
- `_fivetran_index` (INTEGER) shows the order of updates for tables that do not have a primary key.
- `_fivetran_id` (STRING) is the hash of the non-Fivetran values of each row. It's a unique ID that
  Fivetran uses to avoid duplicate rows in tables that do not have a primary key.

### Type transformations and mapping

As we extract your data, we match Tracksuit data types from the Tracksuit database to types that
Fivetran supports.

The following table illustrates how we transform your Tracksuit data types into Fivetran supported
types:
| Tracksuit Data Type | Fivetran Data Type   | Notes                                                                                                                              |
|-----------------------|--------------------|------------------------------------------------------------------------------------------------------------------------------------|
| numeric               | INT or DOUBLE      | We convert the generic numeric type to INT if it is a whole number or DOUBLE if it is a decimal number.                            | 
| int8                  | INT                |                                                                                                                                    | 
| text                  | STRING             |                                                                                                                                    |
| varchar               | STRING             |                                                                                                                                    |
