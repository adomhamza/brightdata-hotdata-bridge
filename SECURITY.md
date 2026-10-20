# Security policy

## Supported versions

Security fixes are released for the latest minor version only.

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |

## Reporting a vulnerability

**Do not open a public issue for security problems.**

Report them privately through GitHub:
[Report a vulnerability](https://github.com/adomhamza/brightdata-hotdata-bridge/security/advisories/new)
(the repository's **Security** tab, then **Report a vulnerability**).

Please include:

- the version of `brightdata-hotdata-bridge` and Python you're using;
- what an attacker could do, and the steps or code to reproduce it;
- any logs or output, **with API keys, tokens and workspace ids removed**.

You'll get an acknowledgement within 5 working days. Once the issue is confirmed, a fix is
released as soon as practical, and the advisory is published with credit to you unless you
prefer to stay anonymous.

## Leaked credentials

If you find a Bright Data or Hotdata credential in this repository, its history, an issue or a
pull request, report it the same way. Treat it as an incident: the owner of the credential must
rotate it immediately, and it will be purged from history.

If you leaked one of your own keys (for example by committing a `.env` file to a fork), rotate it
in the Bright Data or Hotdata dashboard straight away. Deleting the commit is not enough, because
the key stays in git history and in any clones.

## Scope

In scope: the `brightdata_hotdata_bridge` package, the `bdh` CLI, and this repository's workflows.

Out of scope: vulnerabilities in Bright Data's or Hotdata's own services. Report those to the
vendors directly.
