"""Step 7: cross-platform diff analysis via Issue bridge.

Publishes per-function analysis issues to GitCode/GitHub. Each issue contains
source code and objdump -S machine code for both ARM and x86. An external LLM
service reads the issue and posts a structured Markdown comment with line-by-line
diff analysis, root cause summary, and optimization strategies.

This module handles: issue creation (publish) and comment parsing (fetch).
The external LLM service is NOT part of this project.
"""

import ssl

# Shared permissive SSL context for all bridge HTTP clients.
# Needed when running behind corporate proxies with self-signed certs.
PERMISSIVE_SSL_CONTEXT = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
PERMISSIVE_SSL_CONTEXT.check_hostname = False
PERMISSIVE_SSL_CONTEXT.verify_mode = ssl.CERT_NONE
PERMISSIVE_SSL_CONTEXT.set_ciphers("DEFAULT:@SECLEVEL=0")
PERMISSIVE_SSL_CONTEXT.minimum_version = ssl.TLSVersion.TLSv1_2
PERMISSIVE_SSL_CONTEXT.maximum_version = ssl.TLSVersion.MAXIMUM_SUPPORTED
