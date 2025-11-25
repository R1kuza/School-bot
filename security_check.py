#!/usr/bin/env python3
"""
Security check script for pre-commit
"""

import os
import re

def security_check():
    print("🔒 Running security check...")
    
    # Check for .env file
    if os.path.exists('.env'):
        print("❌ ERROR: .env file detected! Remove before committing.")
        return False
    
    # Check for hardcoded tokens in bot.py
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
        
        # Look for potential hardcoded tokens
        patterns = [
            r'BOT_TOKEN\s*=\s*["\'][^"\']+["\']',
            r'token\s*=\s*["\'][^"\']+["\']',
            r'api_key\s*=\s*["\'][^"\']+["\']'
        ]
        
        for pattern in patterns:
            if re.search(pattern, content):
                print(f"❌ ERROR: Potential hardcoded secret found: {pattern}")
                return False
    
    print("✅ Security check passed!")
    return True

if __name__ == "__main__":
    security_check()