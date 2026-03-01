import requests

def is_api_key_valid_http(api_key: str) -> bool:
    """
    Verifies the OpenAI API key by making a direct HTTP request to the models endpoint.
    """
    if not api_key:
        return False
        
    url = "https://api.openai.com/v1/models"
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        # A 200 OK status indicates the key is valid.
        # A 401 Unauthorized status indicates the key is invalid.
        return response.status_code == 200
    except requests.RequestException:
        # Handle network errors or other request-related issues
        return False

# --- Example Usage ---
# Replace with your actual API key
api_key = ""

if is_api_key_valid_http(api_key):
    print("The OpenAI API key is valid.")
else:
    print("The OpenAI API key is invalid or has expired.")
