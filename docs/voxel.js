import * as THREE from 'https://unpkg.com/three@0.161.0/build/three.module.js';

const COLORS = [new THREE.Color('#ffb000'), new THREE.Color('#9aa3ad'), new THREE.Color('#3a3f46')];

function codeShape() {
  const rows = [
    [[0, 9, 0], [10, 8, 1]],
    [[1, 5, 0], [7, 7, 1]],
    [[1, 4, 0], [6, 12, 1]],
    [[3, 6, 0], [10, 6, 1]],
    [[5, 9, 0], [15, 4, 1]],
    [[5, 7, 1], [13, 3, 2]],
    [[3, 1, 2]],
    [[3, 8, 0], [12, 5, 1]],
    [[5, 9, 1], [15, 2, 2]],
    [[3, 1, 2]],
    [[1, 1, 2]],
    [[0, 1, 2]],
  ];
  const cells = [];
  rows.forEach((row, y) => {
    for (const [start, len, c] of row) {
      for (let x = start; x < start + len; x++) cells.push({ x: x, y: y * 1.6, c });
    }
  });
  return cells;
}

function bpmnShape() {
  const cells = [];
  const put = (x, y, c) => cells.push({ x, y, c });

  function circle(cx, cy, r, c) {
    for (let x = cx - r - 1; x <= cx + r + 1; x++) {
      for (let y = cy - r - 1; y <= cy + r + 1; y++) {
        const d = Math.hypot(x - cx, y - cy);
        if (Math.abs(d - r) < 0.55) put(x, y, c);
      }
    }
  }

  function rect(x0, y0, x1, y1, c) {
    for (let x = x0; x <= x1; x++) { put(x, y0, c); put(x, y1, c); }
    for (let y = y0 + 1; y < y1; y++) { put(x0, y, c); put(x1, y, c); }
  }

  function diamond(cx, cy, r, c) {
    for (let x = cx - r; x <= cx + r; x++) {
      for (let y = cy - r; y <= cy + r; y++) {
        if (Math.abs(Math.abs(x - cx) + Math.abs(y - cy) - r) < 0.5) put(x, y, c);
      }
    }
  }

  function line(x0, y0, x1, y1, c) {
    const steps = Math.max(Math.abs(x1 - x0), Math.abs(y1 - y0));
    for (let i = 0; i <= steps; i++) {
      put(Math.round(x0 + ((x1 - x0) * i) / steps), Math.round(y0 + ((y1 - y0) * i) / steps), c);
    }
  }

  circle(3, 9, 2.5, 0);
  line(6, 9, 10, 9, 2);
  rect(10, 6, 19, 12, 1);
  line(20, 9, 24, 9, 2);
  diamond(27, 9, 3, 0);
  line(27, 5, 27, 2, 2);
  line(27, 2, 32, 2, 2);
  rect(32, 0, 41, 5, 1);
  line(42, 2, 46, 2, 2);
  line(46, 2, 46, 7, 2);
  line(27, 13, 27, 16, 2);
  line(27, 16, 32, 16, 2);
  rect(32, 13, 41, 18, 1);
  line(42, 16, 46, 16, 2);
  line(46, 16, 46, 11, 2);
  circle(47, 9, 2.5, 0);
  return cells;
}

function center(cells, spacing) {
  const xs = cells.map((c) => c.x);
  const ys = cells.map((c) => c.y);
  const mx = (Math.min(...xs) + Math.max(...xs)) / 2;
  const my = (Math.min(...ys) + Math.max(...ys)) / 2;
  return cells.map((c) => ({ x: (c.x - mx) * spacing, y: (my - c.y) * spacing, c: c.c }));
}

function ease(t) {
  return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
}

export function startVoxels(canvas) {
  const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
  const code = center(codeShape(), 1.05);
  const bpmn = center(bpmnShape(), 1.25);
  const count = Math.max(code.length, bpmn.length);

  const renderer = new THREE.WebGLRenderer({ canvas, alpha: true, antialias: true });
  renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(32, 2, 0.1, 200);
  camera.position.set(0, -4, 58);
  camera.lookAt(0, 0, 0);

  scene.add(new THREE.AmbientLight(0xffffff, 0.55));
  const sun = new THREE.DirectionalLight(0xffffff, 1.6);
  sun.position.set(8, 14, 18);
  scene.add(sun);

  const mesh = new THREE.InstancedMesh(
    new THREE.BoxGeometry(0.92, 0.92, 0.92),
    new THREE.MeshStandardMaterial({ roughness: 0.55, metalness: 0.15 }),
    count,
  );
  const group = new THREE.Group();
  group.add(mesh);
  group.rotation.x = -0.18;
  scene.add(group);

  const pairs = [];
  for (let i = 0; i < count; i++) {
    pairs.push({
      a: code[Math.floor((i * code.length) / count)],
      b: bpmn[Math.floor((i * bpmn.length) / count)],
      delay: (i % 97) / 97 * 0.35,
      lift: 2 + ((i * 31) % 13) / 2,
    });
  }

  const dummy = new THREE.Object3D();
  const tmp = new THREE.Color();

  function phase(now) {
    const cycle = 11000;
    const t = now % cycle;
    if (t < 2800) return 0;
    if (t < 5300) return (t - 2800) / 2500;
    if (t < 8500) return 1;
    return 1 - (t - 8500) / 2500;
  }

  function frame(now) {
    const p = reduced ? 1 : phase(now);
    const wobble = reduced ? 0 : Math.sin(now / 4200) * 0.12;
    group.rotation.y = wobble;
    for (let i = 0; i < count; i++) {
      const pair = pairs[i];
      const local = Math.min(1, Math.max(0, (p - pair.delay) / (1 - 0.35)));
      const k = ease(local);
      const x = pair.a.x + (pair.b.x - pair.a.x) * k;
      const y = pair.a.y + (pair.b.y - pair.a.y) * k;
      const z = Math.sin(k * Math.PI) * pair.lift;
      dummy.position.set(x, y, z);
      dummy.rotation.set(0, k * Math.PI, 0);
      dummy.scale.setScalar(1);
      dummy.updateMatrix();
      mesh.setMatrixAt(i, dummy.matrix);
      tmp.copy(COLORS[pair.a.c]).lerp(COLORS[pair.b.c], k);
      mesh.setColorAt(i, tmp);
    }
    mesh.instanceMatrix.needsUpdate = true;
    mesh.instanceColor.needsUpdate = true;
    renderer.render(scene, camera);
  }

  function resize() {
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    if (w === 0 || h === 0) return;
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
  }
  resize();
  addEventListener('resize', resize);

  if (reduced) {
    frame(0);
    return;
  }

  let visible = true;
  let raf = 0;
  const loop = (now) => {
    frame(now);
    raf = requestAnimationFrame(loop);
  };
  new IntersectionObserver((entries) => {
    const next = entries[0].isIntersecting;
    if (next && !visible) raf = requestAnimationFrame(loop);
    if (!next && visible) cancelAnimationFrame(raf);
    visible = next;
  }).observe(canvas);
  raf = requestAnimationFrame(loop);
}

const el = document.getElementById('voxel-bg');
if (el) startVoxels(el);
