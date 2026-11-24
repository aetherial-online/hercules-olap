# Security Policy

## Supported Versions
Security updates and vulnerability patches are actively maintained for the latest release branch of **Hercules OLAP**:

| Version | Supported |
| :--- | :--- |
| `0.1.x` (Latest) | :white_check_mark: Supported |
| `< 0.1.0` | :x: Unsupported |

---

## Reporting a Vulnerability (Responsible Disclosure)

The **Ætherial** engineering team takes memory safety, storage integrity, and cryptographic isolation seriously. **Please do NOT report security vulnerabilities through public GitHub Issues, discussions, or pull requests.**

Instead, please report them privately via email:
* **Security Contact:** `info@aetherial.online`
* **Subject Line:** `[SECURITY] Hercules OLAP — <Brief Description>`

### What to Include in Your Report
1. A clear description of the vulnerability and its potential impact (e.g., memory corruption, SQL injection/parser bypass, WAL durability issue, or S3/R2 credential exposure).
2. Step-by-step instructions or a minimal proof-of-concept (PoC) to reproduce the issue.
3. Affected version, Rust toolchain version, and operating system details.

### Our Response Commitment
* **Acknowledgment:** Within **48 hours** of receiving your report.
* **Triage & Assessment:** Within **5 business days**, with a confirmed severity rating and remediation timeline.
* **Coordinated Disclosure:** Once a patch is verified and released, we will publicly credit your discovery (unless you prefer to remain anonymous).
