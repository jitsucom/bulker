# Airbyte - Golang SDK/CDK

This package aims to help developers build connectors (sources/destinations) really fast in Go. 
The focus of this package is developer efficiency. It focusses on letting developers focus more on connector business logic instead of airbyte protocol knowledge. 

## Installation 

```
go get github.com/bitstrapped/airbyte
```

## Docs

- View the godoc's here: https://pkg.go.dev/github.com/bitstrapped/airbyte


## Usage 

### By Example

1. The fastest way to get started it to look at the full example in `examples/httpsource` or the Example in the godoc


### Detailed Usage

1. Define a source by implementing the `Source` interface. 

```go
// Source is the only interface you need to define to create your source!
type Source interface {
	// Spec returns the input "form" spec needed for your source
	Spec(logTracker LogTracker) (*ConnectorSpecification, error)
	// Check verifies the source - usually verify creds/connection etc.
	Check(srcCfgPath string, logTracker LogTracker) error
	// Discover returns the schema of the data you want to sync
	Discover(srcConfigPath string, logTracker LogTracker) (*Catalog, error)
	// Read will read the actual data from your source and use tracker.Record(), tracker.State() and tracker.Log() to sync data with airbyte/destinations
	// MessageTracker is thread-safe and so it is completely find to spin off goroutines to sync your data (just don't forget your waitgroups :))
	// returning an error from this will cancel the sync and returning a nil from this will successfully end the sync
	Read(sourceCfgPath string, prevStatePath string, configuredCat *ConfiguredCatalog,
		tracker MessageTracker) error
}
```

2. Inside of main, pass your source into the sourcerunner

```go
func main() {
	fsrc := filesource.NewFileSource("foobar.txt")
	runner := airbyte.NewSourceRunner(fsrc)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
```


3. Write a dockerfile (sample below)

```dockerfile
FROM golang:1.17-buster as build

WORKDIR /base
ADD . /base/
RUN go build -o /base/app .


LABEL io.airbyte.version=0.0.1
LABEL io.airbyte.name=airbyte/source

ENTRYPOINT ["/base/app"]
```

4. Push to your docker repository and profit! 

### Contributors 

- We'd like to give a shoutout and thank you to @ajzo90 and his initial work on https://github.com/ajzo90/airbyte-http-connector. @ajzo90's project inspired this project 