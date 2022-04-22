"""
File:           dashboard.py
Author:         Dibyaranjan Sathua
Created on:     22/04/22, 5:59 pm
"""
from src.app.app import AppLayout, app


if __name__ == "__main__":
    AppLayout().setup()
    app.run_server(debug=True)
