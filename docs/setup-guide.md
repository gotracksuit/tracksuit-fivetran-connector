---
name: Tracksuit
title: Tracksuit Fivetran connector
description: Step-by-step instructions on how to connect Tracksuit with your destination using Fivetran connectors.
hidden: false
---

# Tracksuit Setup Guide {% typeBadge connector="tracksuit" /%} {% availabilityBadge connector="tracksuit" /%}

Follow our setup guide to connect Tracksuit to Fivetran.

-----

## Prerequisites

To connect your Tracksuit account to Fivetran, you need to:
- Have a valid JWT token for the accounts you would like to sync.

---

## Setup instructions

To authorize Fivetran to connect to your Tracksuit app, follow these instructions:

### <span class="step-item">Generate a JWT token</span>

1. Go to your Tracksuit app dashboard.
2. Click the settings cog, then pick Account settings.
3. Click the tokens button.
4. Select the Generate new token button, give it a name and click Generate token.

### <span class="step-item">Finish Fivetran configuration </span>

{Required}
1. Go to the [connector setup form](/docs/using-fivetran/fivetran-dashboard/connectors#addanewconnector).
2. Select Tracksuit from the connectors then add the relevent fields.
1. Click **Save & Test**. Fivetran will take it from here and sync your data from your {Connector Name} account.

### Setup tests (applies to all connectors but Applications connectors)

Fivetran performs the following {Connector} connection tests:
- Validate the JWT provided can query the Tracksuit API
- Validate the cliet has access to the account brands selected in the forum. Any brands that you do not have access to will be skipped quietly
- Validate that there is any data to be synced

---
