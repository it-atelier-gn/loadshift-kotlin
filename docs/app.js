(function () {
  'use strict';

  var KEYWORDS = new Set(('package import class interface object fun val var by lazy if else when for while do return ' +
    'true false null this super is in !is !in as throw try catch finally suspend inline noinline crossinline reified ' +
    'data sealed enum companion override open abstract private protected internal public infix operator typealias ' +
    'it apply also let run with vararg out').split(' '));

  function esc(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function highlightKotlin(src) {
    var out = '';
    var i = 0;
    var token = /("(?:\\.|[^"\\])*")|(\b\d[\d_]*(?:\.\d+)?\b)|(@\w+)|(\b[A-Z][A-Za-z0-9_]*\b)|(\b[a-z_][A-Za-z0-9_]*\b)/g;
    var m;
    while ((m = token.exec(src)) !== null) {
      out += esc(src.slice(i, m.index));
      if (m[1]) {
        out += '<span class="tok-str">' + esc(m[1]).replace(/(\$\{[^}]*\}|\$\w+)/g, '<span class="tok-kw">$1</span>') + '</span>';
      } else if (m[2]) {
        out += '<span class="tok-num">' + m[2] + '</span>';
      } else if (m[3]) {
        out += '<span class="tok-kw">' + m[3] + '</span>';
      } else if (m[4]) {
        out += '<span class="tok-type">' + m[4] + '</span>';
      } else if (m[5]) {
        out += KEYWORDS.has(m[5]) ? '<span class="tok-kw">' + m[5] + '</span>' : esc(m[5]);
      }
      i = token.lastIndex;
    }
    out += esc(src.slice(i));
    return out;
  }

  document.querySelectorAll('code.lang-kotlin').forEach(function (el) {
    el.innerHTML = highlightKotlin(el.textContent);
  });

  document.querySelectorAll('.stage-code .ln').forEach(function (el) {
    el.innerHTML = highlightKotlin(el.textContent);
  });

  document.querySelectorAll('code.lang-shell').forEach(function (el) {
    el.innerHTML = el.textContent.split('\n').map(function (line) {
      return line ? '<span class="tok-prompt">$ </span>' + esc(line) : line;
    }).join('\n');
  });

  document.querySelectorAll('.tabs .tab').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var card = btn.closest('.codecard');
      card.querySelectorAll('.tab').forEach(function (t) {
        var on = t === btn;
        t.classList.toggle('active', on);
        t.setAttribute('aria-selected', on ? 'true' : 'false');
      });
      card.querySelectorAll('.tab-panel').forEach(function (p) {
        p.classList.toggle('active', p.dataset.tab === btn.dataset.tab);
      });
    });
  });

  document.querySelectorAll('.copy').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var card = btn.closest('.codecard');
      var code = (card.querySelector('.tab-panel.active code') || card.querySelector('code'));
      var text = code.textContent.replace(/^\$ /gm, '');
      navigator.clipboard.writeText(text).then(function () {
        btn.textContent = 'Copied';
        btn.classList.add('done');
        setTimeout(function () {
          btn.textContent = 'Copy';
          btn.classList.remove('done');
        }, 1400);
      });
    });
  });

  var blocks = document.querySelectorAll('.block');
  var navLinks = document.querySelectorAll('.sidenav a');
  if (navLinks.length && 'IntersectionObserver' in window) {
    var spy = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (!e.isIntersecting) return;
        navLinks.forEach(function (a) {
          a.classList.toggle('active', a.getAttribute('href') === '#' + e.target.id);
        });
      });
    }, { rootMargin: '-30% 0px -60% 0px' });
    blocks.forEach(function (b) { if (b.id) spy.observe(b); });
  }

  var stage = document.querySelector('.stage');
  if (!stage) return;

  var reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var tokenEl = stage.querySelector('.token');
  var logEl = stage.querySelector('.stage-log');
  var buttons = stage.querySelectorAll('.scenario');
  var nodes = stage.querySelectorAll('.bpmn-node');
  var lines = stage.querySelectorAll('.stage-code .ln');

  var P = {
    start: [40, 110], extract: [170, 110], boundary: [200, 146], review: [200, 226],
    gw: [270, 110], approve: [390, 110], file: [560, 110], end: [690, 110]
  };

  var TAIL = [
    { to: [P.gw, P.approve], node: 'approve', state: 'wait', dwell: 1500,
      log: '<b>approve</b> <span class="state state-wait">waits</span> for someone in legal to complete the user task.' },
    { node: 'approve', state: 'flow', dwell: 700,
      log: '<b>approve</b> is completed with <code>approved = true</code>.' },
    { to: [P.file], node: 'file', state: 'flow', dwell: 900, log: '<b>file</b> archives the contract.' },
    { to: [P.end], node: 'end', state: 'flow', dwell: 0,
      log: '<span class="state state-flow">Done.</span> The contract reached the end of the workflow.' }
  ];

  var SCENARIOS = {
    first: [
      { at: P.start, node: 'start', state: 'flow', dwell: 700, log: 'An uploaded contract starts one instance of <b>contract-review</b>.' },
      { to: [P.extract], node: 'extract', state: 'flow', dwell: 1300,
        log: '<b>extract</b> attempt 1 of 5 <span class="state state-flow">returns the clauses</span>.' },
      { to: [P.gw], node: 'gw', state: 'flow', dwell: 0, log: null }
    ].concat(TAIL),
    retry: [
      { at: P.start, node: 'start', state: 'flow', dwell: 700, log: 'An uploaded contract starts one instance of <b>contract-review</b>.' },
      { to: [P.extract], node: 'extract', state: 'fault', dwell: 1600,
        log: '<b>extract</b> attempt 1 of 5 gets a <span class="state state-fault">busy response</span>. The next attempt follows after a backoff, within <code>perSecond(5)</code>.' },
      { node: 'extract', state: 'flow', dwell: 1200,
        log: '<b>extract</b> attempt 2 of 5 <span class="state state-flow">returns the clauses</span>.' },
      { to: [P.gw], node: 'gw', state: 'flow', dwell: 0, log: null }
    ].concat(TAIL),
    review: [
      { at: P.start, node: 'start', state: 'flow', dwell: 700, log: 'An uploaded contract starts one instance of <b>contract-review</b>.' },
      { to: [P.extract], node: 'extract', state: 'flow', dwell: 1300,
        log: '<b>extract</b> gets clauses with a confidence of 0.41 and throws <code>LowConfidence</code>.' },
      { to: [P.boundary], node: 'boundary', state: 'fault', dwell: 1300,
        log: '<code>.catching&lt;LowConfidence&gt;</code> <span class="state state-fault">takes the branch</span> without further attempts.' },
      { to: [P.review], node: 'review', state: 'wait', dwell: 1500,
        log: '<b>review-by-paralegal</b> <span class="state state-wait">waits</span> while a person checks the clauses.' },
      { to: [[270, 226], P.gw], node: 'gw', state: 'flow', dwell: 0, log: null }
    ].concat(TAIL)
  };

  var runId = 0;
  var position = P.start.slice();

  function place(p) {
    position = p.slice();
    tokenEl.setAttribute('cx', p[0]);
    tokenEl.setAttribute('cy', p[1]);
  }

  function mark(nodeName, state) {
    nodes.forEach(function (n) {
      var on = n.dataset.node === nodeName;
      n.classList.toggle('on', on && state === 'flow');
      n.classList.toggle('fault', on && state === 'fault');
      n.classList.toggle('wait', on && state === 'wait');
    });
    lines.forEach(function (l) {
      var on = l.dataset.step === nodeName;
      l.classList.toggle('on', on);
      l.classList.toggle('fault', on && state === 'fault');
      l.classList.toggle('wait', on && state === 'wait');
    });
    tokenEl.classList.toggle('fault', state === 'fault');
    tokenEl.classList.toggle('wait', state === 'wait');
  }

  function wait(ms, id) {
    return new Promise(function (resolve, reject) {
      setTimeout(function () { id === runId ? resolve() : reject(); }, reduced ? Math.min(ms, 900) : ms);
    });
  }

  function travel(from, to, id) {
    return new Promise(function (resolve, reject) {
      var dx = to[0] - from[0];
      var dy = to[1] - from[1];
      var duration = reduced ? 0 : Math.max(220, Math.hypot(dx, dy) * 3.2);
      var began = null;
      function frame(t) {
        if (id !== runId) return reject();
        if (began === null) began = t;
        var k = duration === 0 ? 1 : Math.min(1, (t - began) / duration);
        var e = k < .5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
        place([from[0] + dx * e, from[1] + dy * e]);
        if (k < 1) requestAnimationFrame(frame); else resolve();
      }
      requestAnimationFrame(frame);
    });
  }

  async function play(name) {
    var id = ++runId;
    buttons.forEach(function (b) { b.setAttribute('aria-pressed', b.dataset.scenario === name ? 'true' : 'false'); });
    try {
      for (var step of SCENARIOS[name]) {
        if (step.at) place(step.at);
        if (step.to) {
          for (var point of step.to) await travel(position, point, id);
        }
        mark(step.node, step.state);
        if (step.log) logEl.innerHTML = step.log;
        await wait(step.dwell, id);
      }
    } catch (e) {
      return;
    }
  }

  buttons.forEach(function (b) {
    b.addEventListener('click', function () { play(b.dataset.scenario); });
  });

  place(P.start);
  if (reduced || !('IntersectionObserver' in window)) {
    mark('start', 'flow');
    return;
  }
  var starter = new IntersectionObserver(function (entries) {
    if (entries.some(function (e) { return e.isIntersecting; })) {
      starter.disconnect();
      play('retry');
    }
  }, { threshold: .45 });
  starter.observe(stage);
})();
