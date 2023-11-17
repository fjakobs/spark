# Standalone Scala App that executes WASM

On OSX need to manually build the wasmer-jni JAR file:

```
git clone https://github.com/wasmerio/wasmer-java.git
cd wasmer-java
brew install java@11
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.20.1/libexec/openjdk.jdk/Contents/Home

source "$HOME/.cargo/env"

make package

cp cp build/libs/wasmer-jni-arm64-darwin-0.3.0.jar ../spark/wasm/wasmer-embed/lib
```

## Running

`sbt run`