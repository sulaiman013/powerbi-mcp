# Power BI MCP Server - cross-platform OFFLINE subset.
#
# This image runs the platform-independent tools: PBIP/TMDL/PBIR safe editing,
# Best Practice Analyzer, AI-readiness, model analysis, the security layer, and the
# MCP resources/prompts surface. Live Power BI Desktop / XMLA / TOM connectivity
# requires Windows + ADOMD.NET and is NOT available in this Linux image (those tools
# report themselves unavailable and the server keeps running).
#
# Build:  docker build -t powerbi-mcp .
# Run  :  docker run --rm -i -v /path/to/pbip:/work powerbi-mcp
#         (stdio MCP server; mount your PBIP project at /work)

FROM python:3.12-slim

WORKDIR /app

# Install only the cross-platform core dependencies.
COPY requirements-core.txt .
RUN pip install --no-cache-dir -r requirements-core.txt

# Application code + default policies.
COPY src/ ./src/
COPY config/ ./config/

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

# Run as a non-root user. Create the audit-log dir up front and hand /app to that
# user so the security layer can write logs/audit.log without root.
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/logs \
    && chown -R appuser:appuser /app
USER appuser

# stdio MCP server
ENTRYPOINT ["python", "src/server.py"]
