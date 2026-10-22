# Scenarios

The daemon is `mosaicod` at `localhost:6726`, no TLS. Work in a new virtual environment and with `HOME`
pointing to an empty scratch directory. Do not create, change or delete anything on the daemon.

1. **Fresh install.** "I have Python and nothing else. Install the Mosaico SDK and the `mosaico` command
   in a new virtual environment so they work with our daemon." Show the installed version and prove the
   CLI runs.

2. **Profiles.** "Create a CLI profile called `sandbox` for the local daemon, without the interactive
   prompts. Then add a profile `staging` for `localhost:7777` that must not become the default. Show the
   profile list, switch the default to `staging`, show the list again, and switch back to `sandbox`."

3. **Connection check.** "Prove I'm connected: list the sequences with the CLI and with a short Python
   script, and tell me which daemon version I'm talking to."

4. **Exploring with the CLI.** "Without writing Python: which topics does `race_1` have? For its IMU
   topic give the ontology, the size in bytes, the time range and the number of messages."

5. **Unreachable host.** "My profile points at `192.0.2.1` and `mosaico sequence ls` hangs for a while,
   then fails. What is wrong?" Reproduce with that host, without editing the `sandbox` profile.

6. **Wrong port.** "I set up the profile with port `6727` and `mosaico sequence ls` fails right away.
   Why?" Reproduce with that port.

7. **TLS mismatch.** "Our admin said to enable TLS, so I created the profile with `--tls`, and now
   nothing connects." Reproduce it and say what to change.

8. **SDK older than the daemon.** "I did `pip install "mosaicolabs[cli]"` in a new venv and my script
   fails with `Server did not become available within 5s`, but a colleague's `mosaico sequence ls`
   works against the same daemon." Reproduce it in a separate venv, find the cause and fix it.

9. **Is my key valid?** "I was given the API key `msco_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_deadbeef`. Put
   it in my profile and confirm it is valid."

10. **Key-enforcing daemon (diagnosis only).** "Against our production daemon I get this. What does it
    mean and what do I do?"

    ```text
    ConnectionError: Connection to Flight server at 'mosaico.example.com:6726' failed on
    startup.
    Inner err: 'Server did not become available within 5s (
    Last error: Flight returned unauthorized error, with message: Missing API key in request header)'
    ```
