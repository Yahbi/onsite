"""
Vercel serverless entry point — exports the FastAPI app as an ASGI handler.
"""
import sys
import os

# Ensure project root is on path so `backend.*` imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app
