#!/usr/bin/env sh
set -e

engine="$1"
action="${2:-start}"
port="${3:-8080}"
dir="$(cd "$(dirname "$0")" && pwd)"

case "$engine" in
  c7) name="loadshift-dev-camunda7" ;;
  c8) name="loadshift-dev-camunda8" ;;
  *) echo "usage: $0 c7|c8 [start|stop|logs] [port]" >&2; exit 1 ;;
esac

if [ "$action" = "stop" ]; then
  docker rm -f "$name" >/dev/null
  echo "$name stopped"
  exit 0
fi

if [ "$action" = "logs" ]; then
  exec docker logs -f "$name"
fi

docker rm -f "$name" >/dev/null 2>&1 || true

if [ "$engine" = "c7" ]; then
  docker run -d --name "$name" -p "$port:8080" camunda/camunda-bpm-platform:run-7.24.0 >/dev/null
  probe="http://localhost:$port/engine-rest/version"
else
  docker run -d --name "$name" -p "$port:8080" -p "26500:26500" \
    -v "$dir/c8-application.yaml:/usr/local/camunda/config/application.yaml:ro" \
    camunda/camunda:8.9.8 >/dev/null
  probe="http://localhost:$port/v2/topology"
fi

echo "waiting for $name ..."
i=0
until curl -fsS "$probe" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 120 ]; then
    echo "engine did not become ready; check: docker logs $name" >&2
    exit 1
  fi
  sleep 2
done

echo
if [ "$engine" = "c7" ]; then
  echo "Camunda 7 ready."
  echo "  REST     http://localhost:$port/engine-rest"
  echo "  Cockpit  http://localhost:$port/camunda  (demo/demo)"
  echo
  echo "  val backend = Camunda7Backend(\"http://localhost:$port/engine-rest\")"
else
  echo "Camunda 8 ready."
  echo "  REST      http://localhost:$port"
  echo "  Operate   http://localhost:$port/operate  (demo/demo)"
  echo
  echo "  val backend = Camunda8Backend(\"http://localhost:$port\")"
fi
echo
echo "stop with: ./scripts/engine.sh $engine stop"
