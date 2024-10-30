---
name: Tracksuit
title: Tracksuit Fivetran connector
description: Step-by-step instructions on how to connect Tracksuit with your destination using Fivetran connectors.
hidden: false
---

# Tracksuit Setup Guide {% typeBadge connector="tracksuit" /%} {% availabilityBadge connector="tracksuit" /%}

Follow our setup guide to connect Tracksuit to Fivetran.

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related
> to Tracksuit connector and its documentation, contact Tracksuit by raising an issue in the
> [Tracksuit Fivetran Connector](https://github.com/gotracksuit/tracksuit-fivetran-connector) GitHub repository.

-----

## Prerequisites

To connect your Tracksuit account to Fivetran, you need to:
- Have a valid JWT token for the accounts you would like to sync.
- A Fivetran account with the [Connector Creator](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#rbacpermissions) permissions.
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
### <span class="step-item">Finish Fivetran configuration </span>

1. Log in to your Fivetran account.
2. Go to the [**Connectors** page](https://fivetran.com/dashboard/connectors), and then click **+ Add connector**.
3. Select **Tracksuit** as the connector type.
4. Enter the following connection configurations for you Tracksuit connector:
    * JWT for Tracksuit's public API
    * Account IDs to sync, or leave it blank to sync all
    * Filter to apply
7. Click **Save & Test**.

### Setup tests

Fivetran performs the following Tracksuit connection tests:
- Validate the JWT provided can query the Tracksuit API

---
