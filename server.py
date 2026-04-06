import os
import logging
from fastmcp import FastMCP
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Salesforce credentials
SF_CONSUMER_KEY = os.environ.get("SF_CONSUMER_KEY")
SF_CONSUMER_SECRET = os.environ.get("SF_CONSUMER_SECRET")
SF_INSTANCE_URL = os.environ.get("SF_INSTANCE_URL", "https://verdian--partial.sandbox.my.salesforce.com")

INSTRUCTIONS = """
You are connected to the Verdian Salesforce sandbox.
Your job is to resolve company names to existing Salesforce Accounts intelligently.

When given a company name:
1. First use search_accounts to find candidates - try the full name, then key words
2. If multiple candidates found, evaluate which is the best match considering:
   - Similar names (abbreviations, typos, missing words)
   - Same industry/type
3. If confident (>80%) about a match, return the Account Id
4. If not confident, return LOW_CONFIDENCE with the candidates list
5. If nothing found after 2-3 searches, return CREATE_NEW

For Contact management:
- Always use find_contact_by_email first before creating
- If contact exists, update it with new information
- If contact doesn't exist, create it linked to the resolved Account
- Always set newsletter preferences after create/update

Salesforce objects used:
- Account: companies/organizations
- Contact: individual people, linked to Account via AccountId
- Custom fields: eNews_News__c, Send_Whitepaper_of_the_Week__c, eNews_Financial_Services__c
"""

mcp = FastMCP(
    "Verdian Salesforce",
    instructions=INSTRUCTIONS,
)


def get_sf_client() -> Salesforce:
    """Create and return a Salesforce client using Client Credentials flow."""
    import requests
    token_url = f"{SF_INSTANCE_URL}/services/oauth2/token"
    resp = requests.post(token_url, data={
        "grant_type": "client_credentials",
        "client_id": SF_CONSUMER_KEY,
        "client_secret": SF_CONSUMER_SECRET,
    })
    resp.raise_for_status()
    auth = resp.json()
    return Salesforce(
        instance_url=auth["instance_url"],
        session_id=auth["access_token"],
    )


@mcp.tool()
def search_accounts(query: str) -> dict:
    """Search Salesforce Accounts by company name using SOSL.

    Args:
        query: Company name or keywords to search for.

    Returns:
        List of matching accounts with Id, Name, and Website.
    """
    logger.info("search_accounts: query=%s", query)
    try:
        sf = get_sf_client()
        sanitized = query.replace("\\", "\\\\").replace("'", "\\'")
        sosl = f"FIND {{{sanitized}*}} IN Name Fields RETURNING Account(Id, Name, Website) LIMIT 10"
        result = sf.search(sosl)
        records = result.get("searchRecords", [])
        accounts = [
            {"Id": r["Id"], "Name": r["Name"], "Website": r.get("Website")}
            for r in records
        ]
        logger.info("search_accounts: found %d results", len(accounts))
        return {"accounts": accounts, "count": len(accounts)}
    except SalesforceError as e:
        logger.error("search_accounts error: %s", e)
        return {"error": str(e)}


@mcp.tool()
def get_account(account_id: str) -> dict:
    """Get full Account details by Salesforce ID.

    Args:
        account_id: The Salesforce Account ID (15 or 18 character).

    Returns:
        Account fields including Id, Name, Website, Type, Industry.
    """
    logger.info("get_account: account_id=%s", account_id)
    try:
        sf = get_sf_client()
        account = sf.Account.get(account_id)
        fields = {
            "Id": account["Id"],
            "Name": account["Name"],
            "Website": account.get("Website"),
            "Type": account.get("Type"),
            "Industry": account.get("Industry"),
        }
        return fields
    except SalesforceError as e:
        logger.error("get_account error: %s", e)
        return {"error": str(e)}


@mcp.tool()
def create_account(name: str, website: str = "") -> dict:
    """Create a new Account in Salesforce.

    Args:
        name: The company/organization name.
        website: The company website URL (optional).

    Returns:
        The new Account Id.
    """
    logger.info("create_account: name=%s website=%s", name, website)
    try:
        sf = get_sf_client()
        data = {"Name": name}
        if website:
            data["Website"] = website
        result = sf.Account.create(data)
        logger.info("create_account: created %s", result["id"])
        return {"id": result["id"], "success": True}
    except SalesforceError as e:
        logger.error("create_account error: %s", e)
        return {"error": str(e)}


@mcp.tool()
def find_contact_by_email(email: str) -> dict:
    """Check if a Contact already exists by email (exact match).

    Args:
        email: The email address to search for.

    Returns:
        Contact fields if found, or null indicator if not found.
    """
    logger.info("find_contact_by_email: email=%s", email)
    try:
        sf = get_sf_client()
        sanitized = email.replace("'", "\\'")
        query = (
            f"SELECT Id, FirstName, LastName, Email, Title, Phone, AccountId "
            f"FROM Contact WHERE Email = '{sanitized}' LIMIT 1"
        )
        result = sf.query(query)
        records = result.get("records", [])
        if records:
            contact = records[0]
            logger.info("find_contact_by_email: found %s", contact["Id"])
            return {
                "found": True,
                "Id": contact["Id"],
                "FirstName": contact.get("FirstName"),
                "LastName": contact.get("LastName"),
                "Email": contact.get("Email"),
                "Title": contact.get("Title"),
                "Phone": contact.get("Phone"),
                "AccountId": contact.get("AccountId"),
                "next_step": "Contact found. Now call update_contact with the contact Id. Do not search again.",
            }
        logger.info("find_contact_by_email: not found")
        return {
            "found": False,
            "next_step": "Contact not found. Now call search_accounts with the company name.",
        }
    except SalesforceError as e:
        logger.error("find_contact_by_email error: %s", e)
        return {"error": str(e)}


@mcp.tool()
def create_contact(
    account_id: str,
    first_name: str,
    last_name: str,
    email: str,
    title: str = "",
    phone: str = "",
    mailing_street: str = "",
    mailing_city: str = "",
    mailing_state: str = "",
    mailing_postal_code: str = "",
    mailing_country: str = "",
    newspack_user_id: str = "",
    company_name_text: str = "",
) -> dict:
    """Create a new Contact in Salesforce linked to an Account.

    Args:
        account_id: The Salesforce Account ID to link the Contact to.
        first_name: Contact's first name.
        last_name: Contact's last name.
        email: Contact's email address.
        title: Contact's job title (optional).
        phone: Contact's phone number (optional).
        mailing_street: Mailing street address (optional).
        mailing_city: Mailing city (optional).
        mailing_state: Mailing state/province (optional).
        mailing_postal_code: Mailing postal/ZIP code (optional).
        mailing_country: Mailing country (optional).
        newspack_user_id: Newspack User ID (optional).
        company_name_text: Company name as free text (optional).

    Returns:
        The new Contact Id.
    """
    logger.info("create_contact: email=%s account_id=%s", email, account_id)
    try:
        sf = get_sf_client()
        data = {
            "AccountId": account_id,
            "FirstName": first_name,
            "LastName": last_name,
            "Email": email,
        }
        optional = {
            "Title": title,
            "Phone": phone,
            "MailingStreet": mailing_street,
            "MailingCity": mailing_city,
            "MailingStateCode": mailing_state,
            "MailingPostalCode": mailing_postal_code,
            "MailingCountryCode": mailing_country,
            "Newspack_Member_User_ID__c": newspack_user_id,
            "Company_Name_Text__c": company_name_text,
        }
        for key, value in optional.items():
            if value:
                data[key] = value
        result = sf.Contact.create(data)
        logger.info("create_contact: created %s", result["id"])
        return {
            "id": result["id"],
            "success": True,
            "next_step": "Contact created successfully. Now call set_newsletter_preferences. Do not call any other tools first.",
        }
    except SalesforceError as e:
        logger.error("create_contact error: %s", e)
        return {"error": str(e)}


@mcp.tool()
def update_contact(
    contact_id: str,
    first_name: str = "",
    last_name: str = "",
    email: str = "",
    title: str = "",
    phone: str = "",
    mailing_street: str = "",
    mailing_city: str = "",
    mailing_state: str = "",
    mailing_postal_code: str = "",
    mailing_country: str = "",
    newspack_user_id: str = "",
) -> dict:
    """Update existing Contact fields in Salesforce.

    Args:
        contact_id: The Salesforce Contact ID to update.
        first_name: Updated first name (optional).
        last_name: Updated last name (optional).
        email: Updated email address (optional).
        title: Updated job title (optional).
        phone: Updated phone number (optional).
        mailing_street: Updated mailing street (optional).
        mailing_city: Updated mailing city (optional).
        mailing_state: Updated mailing state/province (optional).
        mailing_postal_code: Updated mailing postal/ZIP code (optional).
        mailing_country: Updated mailing country (optional).
        newspack_user_id: Newspack User ID (optional).

    Returns:
        Success confirmation.
    """
    logger.info("update_contact: contact_id=%s", contact_id)
    try:
        sf = get_sf_client()
        field_map = {
            "FirstName": first_name,
            "LastName": last_name,
            "Email": email,
            "Title": title,
            "Phone": phone,
            "MailingStreet": mailing_street,
            "MailingCity": mailing_city,
            "MailingStateCode": mailing_state,
            "MailingPostalCode": mailing_postal_code,
            "MailingCountryCode": mailing_country,
            "Newspack_Member_User_ID__c": newspack_user_id,
        }
        data = {k: v for k, v in field_map.items() if v}
        if not data:
            return {"error": "No fields provided to update"}
        sf.Contact.update(contact_id, data)
        logger.info("update_contact: updated %s with %s", contact_id, list(data.keys()))
        return {
            "success": True,
            "updated_fields": list(data.keys()),
            "next_step": "Contact updated successfully. Now call set_newsletter_preferences. Do not call any other tools first.",
        }
    except SalesforceError as e:
        logger.error("update_contact error: %s", e)
        return {"error": str(e)}


@mcp.tool()
def set_newsletter_preferences(
    contact_id: str,
    enews_news: bool = False,
    thought_leadership: bool = False,
    financial_services: bool = False,
) -> dict:
    """Update newsletter boolean fields on a Contact.

    Args:
        contact_id: The Salesforce Contact ID.
        enews_news: Subscribe to eNews News newsletter.
        thought_leadership: Subscribe to Thought Leadership / Whitepaper of the Week.
        financial_services: Subscribe to eNews Financial Services.

    Returns:
        Success confirmation.
    """
    logger.info(
        "set_newsletter_preferences: contact_id=%s news=%s thought=%s fin=%s",
        contact_id, enews_news, thought_leadership, financial_services,
    )
    try:
        sf = get_sf_client()
        data = {
            "eNews_News__c": enews_news,
            "Send_Whitepaper_of_the_Week__c": thought_leadership,
            "eNews_Financial_Services__c": financial_services,
        }
        sf.Contact.update(contact_id, data)
        logger.info("set_newsletter_preferences: updated %s", contact_id)
        return {
            "success": True,
            "preferences": data,
            "next_step": "Newsletter preferences set. All steps complete. Return the final summary to the user.",
        }
    except SalesforceError as e:
        logger.error("set_newsletter_preferences error: %s", e)
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    from starlette.responses import JSONResponse

    port = int(os.environ.get("PORT", 8000))

    # Get the MCP ASGI app (serves on /mcp/)
    app = mcp.http_app()

    # Add health endpoint directly via ASGI wrapper
    original_app = app

    async def wrapped_app(scope, receive, send):
        if scope["type"] == "http" and scope["path"] == "/health":
            response = JSONResponse({"status": "ok", "service": "verdian-salesforce-mcp"})
            await response(scope, receive, send)
        else:
            await original_app(scope, receive, send)

    logger.info("Starting Verdian Salesforce MCP server on port %d", port)
    uvicorn.run(wrapped_app, host="0.0.0.0", port=port)
