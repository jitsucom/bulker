# Go Workspace

To initialize a new Go workspace, run the following command:

```bash
go work init ./jitsubase ./kafkabase ./eventslog ./bulkerlib ./bulkerapp # and other modules that can be added in future
```

Google is against of committing go.work file to the repository, so it's added to `.gitignore` file.
[Read here](https://go.googlesource.com/proposal/+/master/design/45713-workspace.md#preventing-files-from-being-checked-in-to-repositories)