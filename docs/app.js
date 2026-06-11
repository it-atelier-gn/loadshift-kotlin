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

  document.querySelectorAll('code.lang-shell').forEach(function (el) {
    el.innerHTML = el.textContent.split('\n').map(function (line) {
      return line ? '<span class="tok-prompt">$ </span>' + esc(line) : line;
    }).join('\n');
  });

  document.querySelectorAll('.copy').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var code = btn.closest('.codecard').querySelector('code');
      var text = code.textContent.replace(/^\$ /gm, '');
      navigator.clipboard.writeText(text).then(function () {
        btn.textContent = 'COPIED';
        btn.classList.add('done');
        setTimeout(function () {
          btn.textContent = 'COPY';
          btn.classList.remove('done');
        }, 1400);
      });
    });
  });

  var blocks = document.querySelectorAll('.block');
  var navLinks = document.querySelectorAll('.sidenav a');

  var revealer = new IntersectionObserver(function (entries) {
    entries.forEach(function (e) {
      if (e.isIntersecting) {
        e.target.classList.add('in');
        revealer.unobserve(e.target);
      }
    });
  }, { rootMargin: '0px 0px -8% 0px' });
  blocks.forEach(function (b) { revealer.observe(b); });

  var spy = new IntersectionObserver(function (entries) {
    entries.forEach(function (e) {
      if (!e.isIntersecting) return;
      navLinks.forEach(function (a) {
        a.classList.toggle('active', a.getAttribute('href') === '#' + e.target.id);
      });
    });
  }, { rootMargin: '-30% 0px -60% 0px' });
  blocks.forEach(function (b) { if (b.id) spy.observe(b); });
})();
