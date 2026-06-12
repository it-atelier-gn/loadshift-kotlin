#!/usr/bin/env sh
set -e
root="$(cd "$(dirname "$0")/.." && pwd)"
lib="$root/scripts/.lib"
mkdir -p "$lib"

cp="$root/build/tasks/_loadshift-core_jarJvm/loadshift-core-jvm.jar"
cp="$cp:$root/build/tasks/_loadshift-camunda-7_jarJvm/loadshift-camunda-7-jvm.jar"
cp="$cp:$root/build/tasks/_loadshift-camunda-8_jarJvm/loadshift-camunda-8-jvm.jar"

for jar in $(echo "$cp" | tr ':' ' '); do
  [ -f "$jar" ] || { echo "missing $jar - run ./amper build first" >&2; exit 1; }
done

deps="org/camunda/bpm/model/camunda-bpmn-model/7.24.0/camunda-bpmn-model-7.24.0.jar
org/camunda/bpm/model/camunda-xml-model/7.24.0/camunda-xml-model-7.24.0.jar
org/jetbrains/kotlinx/kotlinx-coroutines-core-jvm/1.9.0/kotlinx-coroutines-core-jvm-1.9.0.jar
org/jetbrains/kotlinx/kotlinx-datetime-jvm/0.6.1/kotlinx-datetime-jvm-0.6.1.jar
org/jetbrains/kotlinx/kotlinx-serialization-json-jvm/1.7.3/kotlinx-serialization-json-jvm-1.7.3.jar
org/jetbrains/kotlinx/kotlinx-serialization-core-jvm/1.7.3/kotlinx-serialization-core-jvm-1.7.3.jar"

for dep in $deps; do
  file="$lib/$(basename "$dep")"
  if [ ! -f "$file" ]; then
    echo "fetching $(basename "$dep")"
    curl -fsSL "https://repo1.maven.org/maven2/$dep" -o "$file"
  fi
  cp="$cp:$file"
done

kotlinc_bin="$(command -v kotlinc || true)"
[ -n "$kotlinc_bin" ] || kotlinc_bin="$HOME/.kotlin/kotlin-2.4.0/kotlinc/bin/kotlinc"
[ -x "$kotlinc_bin" ] || { echo "kotlinc not found - install Kotlin 2.4 or put it on PATH" >&2; exit 1; }

cd "$root"
exec "$kotlinc_bin" -jvm-target 21 -nowarn -cp "$cp" -script "$root/scripts/examples.main.kts" "$@"
