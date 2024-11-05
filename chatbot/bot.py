import requests

# Claude API endpoint and your API key
CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"  # Replace with Claude's actual API endpoint
CLAUDE_API_KEY = "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"  # Replace with your actual API key

def get_claude_response(question):
    headers = {
        "Authorization": f"Bearer {CLAUDE_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "prompt": question,
        "model": "claude-1"  # Specify the Claude model if required
    }
    response = requests.post(CLAUDE_API_URL, headers=headers, json=data)
    if response.status_code == 200:
        return response.json().get("completion")
    else:
        return f"Error: {response.status_code} - {response.text}"

if __name__ == "__main__":
    print("Welcome to the Claude Console Chatbot! Type 'exit' to quit.")
    while True:
        question = input("You: ")
        if question.lower() == "exit":
            break
        answer = get_claude_response(question)
        print("Claude:", answer)
