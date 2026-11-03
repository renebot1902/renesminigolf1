// Rene's Minigolf World – Server
// Liefert das Spiel aus, speichert Ranglisten, Gruppen und die Feedback-Pinnwand (Redis über REDIS_URL,
// sonst im Arbeitsspeicher) und führt die Mehrspieler-Räume (WebSocket /ws) im Arbeitsspeicher.
import express from 'express';
import compression from 'compression';
import http from 'http';
import path from 'path';
import crypto from 'crypto';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const PREFIX = 'minigolf:';

// ---------------------------------------------------------------- Bahnen
// Reihenfolge = Reihenfolge im Spiel. Neue Bahnen immer ganz hinten anhängen.
// ver erhöhen, wenn eine Bahn so umgebaut wird, dass alte Ergebnisse nicht mehr vergleichbar sind.
const HOLES = [
  { id: 'hauptplatz', ver: 1, par: 2 },
  { id: 'murinsel', ver: 1, par: 3 },
  { id: 'schlossberg', ver: 1, par: 3 },
  { id: 'kunsthaus', ver: 1, par: 3 },
  { id: 'erzberg', ver: 1, par: 3 },
  { id: 'klapotetz', ver: 1, par: 3 },
  { id: 'dachstein', ver: 1, par: 3 },
  { id: 'riegersburg', ver: 1, par: 3 },
];
const HOLE = Object.fromEntries(HOLES.map((h) => [h.id, h]));
const TOURS = {};
for (let i = 0; i + 4 <= HOLES.length; i += 4) TOURS['tour' + (i / 4 + 1)] = HOLES.slice(i, i + 4).map((h) => h.id);
const tourSig = (tid) => 't' + crypto.createHash('md5').update(TOURS[tid].map((id) => id + HOLE[id].ver).join('|')).digest('hex').slice(0, 8);
const MAX_SCORE = 8;   // so viele Schläge zählt eine nicht eingelochte Bahn
const MIN_MS = 1200;   // schneller geht eine Bahn nicht

// Bahn des Tages (Wiener Datum)
function viennaDate(d = new Date()) {
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Vienna', year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}
function dailyHole(date) {
  const h = crypto.createHash('md5').update('heute:' + date).digest();
  return HOLES[h[0] % HOLES.length].id;
}

// ---------------------------------------------------------------- Speicher
const cache = new Map();
const dirty = new Set();
let redis = null;
async function initRedis() {
  if (!process.env.REDIS_URL) { console.log('Kein REDIS_URL – Daten nur im Arbeitsspeicher.'); return; }
  try {
    const { createClient } = await import('redis');
    redis = createClient({ url: process.env.REDIS_URL });
    redis.on('error', (e) => console.error('Redis-Fehler:', e.message));
    await redis.connect();
    console.log('Redis verbunden.');
  } catch (e) {
    console.error('Redis nicht erreichbar, nutze Arbeitsspeicher:', e.message);
    redis = null;
  }
}
async function load(key, def) {
  if (cache.has(key)) return cache.get(key);
  let v = null;
  if (redis) { try { const s = await redis.get(PREFIX + key); if (s) v = JSON.parse(s); } catch (e) { console.error('Lesen', key, e.message); } }
  if (cache.has(key)) return cache.get(key); // inzwischen geladen
  v = v || def();
  cache.set(key, v);
  return v;
}
function save(key) { dirty.add(key); }
setInterval(async () => {
  if (!redis || !dirty.size) return;
  const keys = [...dirty]; dirty.clear();
  for (const k of keys) { try { await redis.set(PREFIX + k, JSON.stringify(cache.get(k))); } catch (e) { console.error('Schreiben', k, e.message); dirty.add(k); } }
}, 700);

// ---------------------------------------------------------------- Hilfen
const cleanName = (n) => String(n || '').replace(/[^\p{L}\p{N} ._\-'!]/gu, '').replace(/\s+/g, ' ').trim().slice(0, 16);
const lc = (n) => cleanName(n).toLowerCase();
const rate = new Map();
function limited(ip, max = 40) {
  const now = Date.now();
  const r = rate.get(ip) || { t: now, n: 0 };
  if (now - r.t > 60000) { r.t = now; r.n = 0; }
  r.n++;
  rate.set(ip, r);
  return r.n > max;
}
setInterval(() => { const now = Date.now(); for (const [k, r] of rate) if (now - r.t > 120000) rate.delete(k); }, 60000);
const cmpEntry = (a, b) => a.strokes - b.strokes || a.ms - b.ms || a.at - b.at;

function boardKey(kind, id) {
  if (kind === 'hole') { const h = HOLE[id]; return h ? `board:hole:${id}:v${h.ver}` : null; }
  if (kind === 'tour') return TOURS[id] ? `board:tour:${id}:${tourSig(id)}` : null;
  if (kind === 'daily') return /^\d{4}-\d{2}-\d{2}$/.test(id) ? `board:daily:${id}` : null;
  if (kind === 'mp') return 'board:mp';
  return null;
}

async function groupMembers(g) {
  if (!g) return null;
  const groups = await load('groups', () => ({}));
  const gr = groups[g.toLowerCase().trim()];
  return gr ? new Set(Object.keys(gr.members)) : new Set();
}

// ---------------------------------------------------------------- HTTP
const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);
app.use(compression());
app.use(express.json({ limit: '20kb' }));
// Das Spiel (index.html) darf im Hauptverzeichnis des Repos oder im Ordner public liegen.
const GAME_FILE = [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].find((f) => fs.existsSync(f));
if (!GAME_FILE) console.error('index.html fehlt – bitte ins Hauptverzeichnis des Repos hochladen.');
app.get(['/', '/index.html'], (req, res) => {
  if (!GAME_FILE) return res.status(500).send('index.html fehlt – bitte ins Hauptverzeichnis des Repos hochladen.');
  res.setHeader('Cache-Control', 'no-cache');
  res.sendFile(GAME_FILE);
});

// App-Symbol und Manifest (zum Installieren auf dem Handy)
const ICON_512 = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAYAAAD0eNT6AAAoW0lEQVR42u3dW6xk2XkX8G/VuKWW+sXjF15aGttcBmLHY2NH9mDskEweHBtbeYiQLxISQgYikCzxgIQAYUTAUh4ijBITYkXwgMeGOAFiZIWAFSCybC7GTEiAgYAZqV94Ghtn7M4MXYuHuuy19157165z6tSpqv37ST1z+lTV6dNVXef7r29ddgpOyrt++g35mH/evcfvetJn7KUXH3oSZuzY7/9f/vDXkmf9dHgxLrzACwAIAJzL+19AEAAUewEAAQDvf6FAAFDwBQAEALz/BQIBQMEXABAA8P4XCAQARd8PAAQAvP+FAQFA0RcAEADw/hcGBACFXwBAAGDO739BQABQ9AUABABm/v4XBgQAhV8AQABgxu9/QUAAmHXhFwAQAJj7+3/uQSAp/H4AIADg/S8ICAAKvx8ACAB4/wsCAoDC7wcAAgDe/4KAAKDw+wGAAID3vyAgACj8fgAgAOD9Lwicp4XiDwDD3vPsW7MOgMJvBIAOAN7/ugECgMLvBwACAN7/gkCYAlD8AQjTAgKA4g+AEBCmABT+0AIkTAHg/R+mBHQAFH8AdAMEAMUfACEgTAEo/KEFSJgCwPs/TAnMsAOg+AOgGzCzAKD4AyAEzCwAKP4ACAEzCwCKPwBCwMwCgOIPgBAQ89kFoPCHVcCEXQB4/4cdAnPqACj+AOgGzCwAKP4ACAEzCwCKPwBCwMwCgOIPgBBweyFg4ekHgPlZGP0DwPy6AAvFHwDmFwIWij8AzC8ELBR/AJhfCFgo/gAwvxCwUPwBYH4hwDZAAAjbAI3+AWAGXYCF4g8A8wsBC8UfAOYXAqwBAICwBsDoHwBm0AVYKP4AML8QsFD8AWB+IcAaAAAIawCM/gFgBl0AHQAA0AEw+geAOXQBFoo/AMwvBJgCAIAwBWD0DwAz6ALoAACADoDRPwDMoQugAwAAOgBG/wAwhy6ADgAA6AAY/QPAHLoAOgAAoANg9A8Ac+gC6AAAgA6A0T8AzKELoAMAADoARv8AMIcugA4AAOgAAACzDwDa/wAQFzkNoAMAADoARv8AMIcugA4AAOgAAACzDQDa/wAQFz0NoAMAADoAAMAsA4D2PwDExU8D6AAAgA4AACAAAACXHwDM/wNAzGIdgA4AAJgCAAAEAADgsgOA+X8AiNmsA9ABAABTAADAHLzKUwBcumc/+djej/nwxx554tABAAAuMABYAAgAMauFgDoAAGAKAAAQAACAsAsAIOa3c8COAAQAAGHAE0KYAgCYYRi4yrkCoAMAoCsAty45A2De7j1+15MwYy+9+NBJgAd2TmHA+18HAACdAcIaAACsF0AHAABdAQQAAIQBBAAAhAHCGgAArBdABwAAXQEEAACEAQQAAIQBwhoAAKwXQAcAAF0BBAAAhAEEAACEAcIaAACsF0AHAABdAQQAYL7uP//cXvd/8ORTMwkDr8RHP37HPxABAIC5+fTHX9l+LAyENQAAzDMMlIEAHQAAdAUQAAAQBhAAABAGCGsAALBeAB0AAHQFEAAAEAYQAAAQBghrAACwXgAdAAB0BbiZAHDv8buehRl76cWHnoQZ8/7nFMKAixPpAAAQrlRIWAMAwPzCQBkI0AEAQFcAAQAAYQABAABhgLAGAADrBdABAEBXAAEAAGFAAPAUACAMhDUAAGC9gA4AAOgKCAAA5+fBk095EhAGBAAA0AUQAABQ9AUAAFD4BQAAUPQFAABQ+AUAAFD0BQAAUPgFAABQ9AUAAFD4BQAAUPQFAABQ+AUAABR9BAAAFH4EAAAUfQQAABR+BAAAFH0EAAAUfgQAABR9BAAAFH4BAAAUfQEAABR+AQAARR8BAACFHwEAAEUfAQAAhR8BAOAW3X/+ub3u/+DJpxR+BAAAFH0EAADO3Ec/fmf1wYsCgAAAwDyKPggAAAo/AgAAij4CAAAKPwIAAIo+AgAACj8CAACKPgIAAAo/AgAAij4CAAAKPwIAgKKv6CMAACj8IAAAKPogAAAo/CAAACj6IAAAKPwgAAAo+iAAACj8IAAAKPoIAAAo/AgAACj6CAAA5+3Bk08p/NCx8BQAgAAAAAgAAIAAAAAIAACAAAAACAAAgAAAAAgAAIAAAAAIAACAAAAACAAAgAAAAAgAAIAAAAAIAACAAAAAAgAAIAAAAAIAACAAAAACAAAgAAAAAgAAcEpe5SkALt3955/b/0GffZsnDh0AAEAAAAAEAABAAAAABAAAQAAAAAQAAEAAAAAEAABAAAAABAAAQAAAAAQAAEAAAAAEAAAQAAAAAQAAEAAAAAEAABAAAAABAAAQAAAAAQAAOA2v8hQAl+7Bk0/t/6CP3/HEoQMAAAgAAIAAAAAIAACAAAAACAAAgAAAAAgAAIAAAAAIAACAAAAACAAAgAAAAAgAAEC4HDAn5ksf/fq1v8Yzn36LJxJgagB46cWHnoUZu/f43aP+eb/0o185eoj4wOef9kIPmM/7/zHPzQm8/9EBYCZusthf9/sQCgABAC6w6O/zfQoDgAAAF1z0hQEAAYCZF31hABAAQOEf/bsKAoAAgMI/47+7IAAIACj8ggCAAIDCLwgACAAo/IIAgACA4n/5z5sQcNruP//c/g/61Bs9cQgAKPzoBgDhaoAo/ng+AR0AYp5X30M3ANABQPFHNwAQAFD8EQKAMAWAwi8EmBIABAAU/wv24MGD3ufu379vuyAQpgBQ/C+08NeKf3mbKQFAAODki/+dO3cm/1L8H0y+nxAACACcXPG/alGfcxiYWvyFAEAA4KSK/6GLt85A2CEACACcfvG/SZceAvYd/XcfJwQAAgBHLf7HHKHrBugEAAIAJ1L8b4MQIAQAAgAzK/5CgBAACADMtPgLAUIAIAAw86J7KSHg/v37R30cgACAU/50AQAEAMX//Ebbc+0CTLm/EAAIABj5X1AI2CcsCAFAuBoglzrKvnPnTrzyyisXFQKGrgYIIABg9B+Hv9zuuS8MHOoCuIQwEKYAFH+X232w921hUSCADgCXf7ld7XZ6/y6efGr/B33M84YOAEb/Z3e5XV0AAAFA8UcIABAAmNPldgEQAGY5+j/1LXaXsgVQFwAQAAAAAYDbn/s/1VG20b8uACAAAAACAIde+X9qo+2p34/L7eoCAAIAFxICtP4BBACOvO//tovvVf78m7jcri4AIACgE3AGf+5NXG4XQABgdqf+vfLKK0cLAof6s+7fvz9Y4Mdu0wUACBcDol+c79y5c1bdBqN8AAGAAxfpQ4QBi/wAwhQAcVYX/dm06/ct4ld9HGEaANAB4PQ6A1MuqqMlDyAAcGE2xb0WBBR+AAGAOP/2v1F+7H2J4VN5Xn7pR78SH/j8095IgAAAN1n4u7cJSEBYBAjzKP5XuR+AAECcS/tf8Y+zCAF2AwACAAAgAMBNj/5NBQACAAAgAAAAYRsgYQEg4TyA47r//HP7P+hTb/TEoQMAAAgAMJsjkMOJiYAAAAAIAKALYPQPCAAw5xCg+AMCAGEHwOWFgKECP3ZbOBIYCNsAYbYLAwF0AAAAAQAAEAAAAAEAAAiLAIkbuMTtbS1sO6XvBUAAYHbXtt/cdqzie0rfC0CYAmCOxf8q97uU7wVAAGD2xf8YhfeUvhcABAAAEACY7+j/Jkfep/S9ACAAAIAAAAAIAACAAAAAhIOAiDO4dO1VFtHdxCE8p/S9hJMZAXQAbtszn36LJyHmuUNjKBCN3TbVBz7/tCcZ0AHQBXhwEiPuU/pezuVkRt2AAz7vTz61/4M+5nlDB4AzDwGnUnBP6XuZ+8mMADoAMwoBpzDnfErfC4AAgG7AjL+XY4SR65yGKBgBAgC4NDFAWAMQdgLg0sRhBwAgAIDFeAACAAAgAMBtXZr4qmsJrEEABAAAQAAICwGJy96Gue/9LQAEBABwMiNAOAcAnMwIIADEiU4DfOmjX/dEzOwyyWH/PyAAAFzdFz7y5cHbvv3Jdx/0673/M+/0hCMAgMskc1uF/dS/J0EBAYAwDXCeIeAciv+ltP9PsdDf1N9JMEAAAIvxjOg9B4IBAgDM/dLECr7nSiBAAAjTABBn2P5X8AUCBADAKP9onnjiicHbfuN3PXHQr/fCCy/c+vMsDHBd6V0//YbsaTg9ugAcY/T/0osPT77ojxXiqa4SAN74f65f5I8VFK4aBu49ftcbSQcA4PaK/iGK/Ck6VhdBZwAdAF2AOMXL7Vpkd7pz/1M7AIcs/LdR7G+rA3CbHYMpQUAHQAcAbqTwd28TBOY54r/U0f0xOgbXCQSb105HAB0AXYBbKf66Aae98r/WAbhu0T/Fgn8uHYCb7hB0w4AOgA4AHKX4b+4vBFzeiP/UR/n3n39u/we9+tUn3SG4Shj4wke+rCOAABDOBcC+/2sVfq398wwDpgYQADj66F8X4PxH/Ir+5YSBzWv/wS8+4wmcqYWn4Dy6AHDo0f/n3vulvQqM4n8+YWCf12qffweERYCEBYHX6ABEWAx4m8V/38J/Cb75zW/G/ksAXn0Rf/d9pgd0A3QAgAs1tfgb8c+zI6AbIAAQpgK4vNH/lB/uCr8gIASEKQDCVEDczDTAsdr/l3Aq4bGK/6UX/TlPAcQVpwZMB+gAQJzj7oShYDJ2W8y07W/Eb+eAToAAQJgKuOnR/E2PwKcW93MIAYcY/Sv+CAEIAELArYeAUyn+5xACbrr4f/CLzyj+bEPAWLtfCBAAEAJ2FvehAj92G8c77a8s/l/5sd/yRLP1lR/7LXP+AgAcJgiUv+LETyWMmc37K/7s++9CF0AAQBeAM2/9a/sTV1wTIAQIAAgBnGnrX/HHvxMEgEuQ27+e+Vkh4KyL/88/3XtNAQQAJhUHIeCMi/8VX/OY2Ka1wIvYc6GoaQABgFMo9nuMBucaAq662PC2dye8/+efnv5SX+PfhYV/RPj3Qtur7j1+17NwCgX/OnfL7Vve/4/eEV/4Y1/1vJ64H/y7b+59LqXkieGkvfTiQ0+CDgAHG+Xv0/XPufKrf+f3/8N36AKc8Oj/B3/mzdXXPufc+7XXP6srPg6YYQfAU3Bao/28Y3Q/Ghg6t/3Rz74j/tmHvjq7EDBlb/9tFv8f+DtvjtR9PcuBf+oX9at0CTaP01W4/Av7gA7AOYz4pzQDytH9SLeg1QRYlvdbfTLnHO979u2zPYwoTvBUwh/41FPr16bTvClf4+V4h2jfUb5uACAAnFDhH2/t1wtAWeybgt8U++2vZTsQvO8fvD2cSnj7xxH/kZ96qsl1lamc1sufi1CwHAuB7TAwFA7e9Dccxcx0b/qb/r3MQXrPs281PDhiuz/v0d5v3ZzbH2xvy/U/IEdu377+/xf/+L/zuhzZ9//tN9Vb8an9/9SbAkj12yuPLf3TD/9a68/afPza177Wi8EkL7zwQvXz7//MOz05OgDsu7ivOtqv3L/f0u+M8MvR/fr/q893f0Xn1+rzP/z3v89rdETv/ltvWr1exWvQvJarX7G9vejwrF/XsQ5BrztQ6Qx0PwYQAI444h8t/OVNRdGfVPBzUexzu9DHMkd+tL79UREIHuV4z8+9zWt1BO/6ye9tXovytXoU21CwDQZFKNi+1p1AUJ0yiM76gTxc/L/xjW94Ubjy6J8wBcD0lf251svPlRZ/7rT2O63/HLnVys+t1v7Q43J1GiGXD84Rv/Knv+b1O7A//BNvbLfty9Z/6rT0y3Z/6nwuVVb9Vx+fyi8RERH/+MP/evu47nTA6173Oi8Sexd/7X8dAPZd91cZ8ddH++2Fe+22fnuEH8vOyP5RMdpff375KGJZdgD+Xy5+H9v7v/MTb/CCHdA7P/GG5jl/FIOvTzxqdwOGpnCqHYKyO1C2AIquwI88+/2xXC6riwQtCCT2XPin+IdzANg9+u8t8qst7FuPwluj9nKkX47yayP89Q/63qi+2iUY6SzkVTH6Qz/+PZEj4it/+b94Pa/o6R//nmIwvxmRr5ZiplRZ3JfWL0JqfyoXj213AlJxn/VteTWiz0VnIBUHCwzN+/+TD/2b+JF4d/z6X3rghWNb/L/wkS97IsIUANct/jnX2/1TC/9ypOh3C36n2A8V+nZYaL7md771cuu7/+pf/a9e1z2946/9gVaF7xX81JkKSFG5X2r/vnxM6/ed6YJFZ5qgc99f+OCvRkqp9au8/+tf/3ovYGj7jzH6FwA4dPEvCn/zuXXhX44U/WWn4C+LP2eZK/P9TSeivXZg9ed/dxsA2gsG/u1f/29e4x3e/ld+f3k8X/vD3sl+IwV+U9OHAsGic99FEQZSKj6uBIFKCNgEACEAxV8AEACuGQCGiv/QqD8Xh/3k3C70vaLfKt6dAFEb2XeLfafD0A4lOb77f1+O6hkC64///See91p3fN9ffLIY2Vc26af2ur9IqR8MJoSCtCi/XicolJ2B8mt1g8A6LPzCh361Vfi7gUAIUPwVfwGAmyz+m2K87LT6l93CX470K0W/LPjLPFrsc3cdQbSnFb777Zfb9x04pOg//MR/n/3L/ra/8Pu6l+7rf9g73Ce1pwLSxFBQmQZIteLe+rjoCJRTA5tOwI4QYIeAwq/4CwDs2/qfWvzL4p2b9v1m7/e28OdulyAPdAuK+40V+9wNCU2Rf/jtl+unCg6cKPgff/J/zO4l/4N//vf2i/rABXy6Bb5W5HeGgtrIv5gGaG5fh4UiCKRFMTVQBob1x784IQSklJwaOOP9/Yq/AMC+o/8JxT/n9Yh987mRwt/cVhT9ZdNBKAt+r9h3R/TV72/1gLIDUNvHmAc6Al//5G9d/Ev9lo/9nuqIv3VSb/co3m6BHxv5t1r/zWN6BX5o5F9MB7SCwMjnayFgsVj01gU4PnieB/so/gIA1279Tyv+Zet/W/DLwr/MTSgougXtLYB5pBvQOk2oN9LPOcfD335l8LCinAdPNmp9+j/91P+8mJf4zX/ud0f1wP400P1PAwGhuhNgPBi0W/+dOf7W7fWCnxad4r8YCwerxYGLxWJwh0B5gJAgoPAjAAgAE0b/3aLcKv7L4tCfZb/4b++zbHcDWnP+OUa2DLZ3BnQXC3YXATYBYOBiQzE8PdCaIlh//td/5n+d3cv6pj/z+lZhTpH6F90ZCwGVkNDfCdBerd+6UE93OqCzPiClShioFPfyc5uPt7cNhIBf/NC/Gp0O6F5Q6IknnvBz4MKO8lX8BQCuEgC6o/9luwBvW/u9kf+m5Z+LU+BGAkFrmqCcGqgdClTvSPS3JK48/O2Xo3ptgsjDxxzn4Qsalv7zz55uGPjePzW86j2NFP3qWoBqYe8e+lNbD1AJBakzn1/O+6dKi3/R/H9V5Dcfp2owiPK2iLj3mrsRkeIz7/3nMbRNsHolQ2Hg7M/v/+AXn4mXXnzoyRMAuLHR/6bN3yv07c+1gkE3MOQdOwais21w4NoAtcsLbwPArvb/rmqfp939N37u+BekeeOffN14kR8r9iMPSEOX5B3YATAUDFqt/TIQpAlz/YtKF6AIAWVAiEVqTRVEirj3+N3Wn/GZH/6VnV2AGmHgfC7a88EvPrP9WAAQAASAQ4/+u63/KxT/fhhof+3mz91xVHDvBMDcOgzo4UuvDFfriSP9sQsijdzQ+rq/+ff+97Vfojf8idcOF/cplX7H43Z3BtKONQKdHQStUNBfBNhu/W8Kfjnibz6XFmk8BFQ6A9sAsFjHlfWf++x7/8XkLoAwcF5X6iuLvwCAAHCdrX8Do//Wqv7txXv6Bb9b/JePcnuaoLVYMEa2Aw5cGyAqK/o7VxH8nXUAyHlK7c6TGwJXyAI3/C99z7unHV8gxR4dgfFTAssFf929+9UtfYti0V9R5Fu/f6wzPdANCOuvde81d/udhuKUwc+99192/ipp76deILjdS/N2i74AQLgY0EGv/NtrEZQhoblqW7mwL7ZX9YtlJyjkgTUBuXJ+QOy6PkA05wDUwsyy/ncYLe75agX+Vut/3i8g5Nz9fD8FtmthqjxmUzTz9vPNfZqFfjmtX7B1GNhe7Gdb/HPkdZHOi/WOkLS+GlBOEYv1gyOvHr+I9ZRRirRY3yev/582/6+9OqlSPH4oUkR8thMErlPEBIKbLfhTCj8IAIdOBXmsOHbm7cu9/eUlf8viX9sJkIu2/3Kg6A8V/FwsToz+x7tW/E/r+OczC26bKjyQCfJwWMh5ICDk9lx/bupzU/tzbgJBJQyklCKnzVUB1wV7kSOWq25A3ny8LvCrrtN6FJ+bop9z2v77SLnJDL1vOw1mgIiI+NAXfygiIj73vi/dSJGbYyg4ZLFX+BEAbrOIVA7S2YzYm2BQjP6LdQK5/Lg8GyC3FxFGeZ9K4e9t98sDRT7HhO19By7sp5AL0n6hJQ/1+HMlIHTa+rmTwFLUA0ErDKz+s84jRRBY5NXdl+tuwCJtOxp5ue4ctA59StuwmLb/OFIzzs/FsdETX7pUKS6fe++X4hjF8JzDwU0VeUUfAeDWkkC/VZ57R+e22//bLFCu0l92AsKyCQibtv9g8V9WRvxjhX/kcJ9rFfu819N16wlu0hT2xKCQx0LBSCBYFf0U/fyQN/dYfa+L1WuYlqu2foq8avKvR/t5kVdz8puiv2nv56ILUH4/lWmAvJk22ASP8jnK40/YTYWBqxbRYwaFYxV2RR8B4MzaAbn2mc4q/cEtep1wkXP/iOGy+Ofe15pY+Mu1AVMqdD6TIn+NMNIUvj2CQTcUbNr5u6YOyiCQmtc3pSap5OU6BESOlFOTK4o2fs5l0S8vK92ep8h5z7WQeb+Fk8cKA6delBV9zkmKiLATYFrxm7L/vzeHv1nx/6j8uPn98lGxI+BR7j+ue3pgdydAZ+Q/bdTffGKzC+DSC//1dgDsuaOgXPUfUb1KYPuulTMBYmj1/8DWv8ea1f5pEevfl1sAU6THNvdtvt6915TnANR3AlT/Hnu4rUCg4O9mF8B8/dqf/c2kAwActYgJBEb4hCkA2lPI28XoxRawcnJ2e3surjhTDsE328jKfm8xt9v/Q2s3Dox2847vPxv5V68YuMfov/rNFKcCts8SSINfd+8DjE6kyM0pGCj0CADnPnmSp960XnhVXhK2nCTe7PWOaBXkTft3W6cjtvu+03IzD5yapYe5fdpczrl3nZpuEEjFvHWM9fXT+BTBWPE8pXBwrUWAE75gtVU+VPgjBlv/rRMBI3UOBirvuj7eN3WCY/TPG0g3eHCSYKDQIwDMZ9heFOnWCur1Wu1NkW//YF89IK8XgOVFNKuy1xdyiZy3R7PmZV6f5JZXW8AiRyxW28FikZutYGm1LSz3YkclCHTTSXVPeJq+MyBNrJGnvA3wusV+R8Efu3Jgdb49dU4FXHQvCVysByiqe4oyDFQ6BNvPpd4FjVKqdQtSnFMRPWZQUNgRAAz6RwvN6mCXosCvi/1qe9dm0VVufvinpsCnzQEwm5PcYnWoSyqG+qs94E0QKOcIcmsKoL1VrRUUUr3dXwsF1zoIKJ1f7z9NCRJp15ccuFRwVEb8leOAq9cBKDsBi8rofx0Cmq+X2t9Gal918ASbAYoyCABnlgzG5su7R7qmWB/q0rTxtwsBFtvd2av93cu8WvGdu8fPrT/Om+7DZrthf/93NRD0znavHVk7sJ9+cOtYOs9LAaT9H5iGWgBpwjUCKkW/e12ANHAdgM3Hm0BQ/r41+i+mB2rTB1G5FDEgALB3FyC1CujqrJX2NEBT/NddgGIGvzyMJZbFqTXLpsg3B7lEe2HAdltiJwxsDwpKzXfXOW9gM8+cc+e8ujRyMaChhX/pmufxuxjQgS4G1GwHLMNAtxsw5QmSCeDCzwGIcBZAuBzwrV4O+LgL/VwOeOxywK1zCKSBi+YcgJjtGQA6AAdaCNg0AJrV/dux/XoRX5Rj/WW0ruDWHAS77gks82pBYCrusjkVbrnuLuT2tEMqDgdqOgbl6Du3R/m5XWy6n8+50tXOex1/2OsqXHlV/rGm/dN+Dxge8Udr9X510V/t8J9NwU3RL/xlJ6BV/Nufqxb/cvFgageLwa5AUu0hTAFQ6fA39a6z136bC1InJ2xatusleGmZeiFge0T8Zgphswsgr3cMLKM5x7242Mt223/n46YzUHz9bjB4LLWvZJiGjj/sh4T6ZXP3CA0nuhdw90i/vYK+N5c+UOirJ/1V5uFTZxtgbAt5JRC0OgL9kX7rPtFdb1BZ/T+0lEEeAAGAiV2AXFzbfZEiLXOzRm6xPtd9uRq5NTsGijCx7ASBvG7dri8bvLmEfCyjVdjTdjqiHwrKxXubYLAKJa1zjtf/SxMvEZx3dPtTTLp+Qr7hur7HIT9pqAqmPVf6p/6ZC/2gkDrFv79AsD0V0Cnq3dH/ZuHfotkuWO0chNE/CAAcrDuwWZOXWld5W4/6NyFg0Wzl217ZLa1X+m8W/aV2sd98vK3L5Yh/UXYCVkFhs82we4W6bhDIuewAFEP54vZ6CMitYjlYvHNMWg6weiYO9VJMnN9Pey7+22dlf1RW1qfOlEBZ8KOyvS+GdwNUC/qmw7QjJDS3N8V/cCoDmMciwAgLAeMqiwEHLg602rfftNZbiwI3n1s2FxCKZXG54NZtxaK+5XoCYdkexZeFvfx9zmPf3+oB3/32y/VCnSt/x6ssEsyx18LAq3QCxgesafoawImL/FoD/0qw2FXo+wcAtR/Tav2nyr79xYS1AQOfb76XYhHg2MV/BIOwCJC4wAWAOgCH3BLY6QJELo73LToBUdvPv2z266dlam4q5vg3o/3+qL8731+M7otivtkCmHJ7IL94LLUDxNBoP6fhOp4HjkDIO66rm29w29lVrtgXO/bEd+f+U/3wp9Gtf6m/ODANHAHcK/rleoCoFPjNY2tbByvFP02Z+wfCFADDBwE05wDvDAE5bc7xT81e/c3JO8Ue/7RMzXbD4nrvubvwb1kp7iOhoLUffz0FkGqXDc65c5zwyOC9tiZgwg6A0R2C+ZpFfo8z/9Ou6pcGOg1j2/4GdgJUOwGtx6T+oT2d1n2vE7D9uFhrsBg4UyC6awCidyKhgwBAAGCfHQH7hIDNOf6L9eE/i+KY31YQ6Bb2bhhYLzDMneKex0NBKxgsUu+gnhy5N+KvhoTKSL8XGK56BGC+oTNq066tgPtt9xvqBtTO1B8s9lEp+EOdgNQ97a8Z1Q9OF8RA8a9dgEjxh3muAYiwDuCqBao3V14ptr11AVHO8zeP3a7iz8V6gU0VL0b824OHon0oUO/wn826gk7bPkeO737r5cGWfXfLYH8NRB4v1vmaq/xHL1O8f3WadFncKV2BNKEb0Puzxot9fzFecf9F575lUe/sLkidywinHaP+e4/frRd/ASCsASAueP5fB+CAUwFDnYD2NXpq6wKiGabntLnMz7r+Ne351UE+RWdgPUovrv+zc/V/yp1di+UugOK6AtUthDFheiDGL5HcD035xi5DkyYc3TupKzDUEaiGiTQ4HdAt9v37jQSE1u87BX3RuabD2H3T8DHFij+EKQAOFAI6x+5urweQNyO2ZrX+tpCmYmpg284fCQOtFft5e+Z/e7TfXGege5W/nCMWj21+m3o7AIbn99NgcR8d6ec8bcvekQ//SRMW/vWukxDRP9K3e4fBQ3bStJAwoZBPLvxROX9g6K+t+IMAwDVCQPeKOtVuQBEEUkTqjfDLrkC0dw+UI/0yEHRDQaTq6v9NMNh0ANLAQr9UFO3cXb0/sIo/Dx4UlE5ksivtzgW9xXwDYSKNTS+k8bDQCQpTR+7DRX/40r/tb1/xBwbe7tYBxEEWrOXayLc2L15MzufKCDx3LuCTK1f16z8uV9vzuXxwjvjOt35nYK6+Pr+/s93fXUdwhYOBbuRf9thC/x2r31Ma7wpUi3+t0FdDQLtw10bsaefjU//U4TR0xcL2J+89flcACGsAiNnN/+sAHLrY5IHmQCpO2yvuW7aB82Z1f2pPD7S2kpWtgNyMqvsdgtWjWmcBtEb0zd7/chdA5Fx0MFJ1KiBV6/zwKD+Nrek7YgCYfkxwmlD4R9r+Uwr9jmKdKh2G3v2HRvsxrfDb7gcIADd8RkArGwwEgfYagc7seMrtq/PlkUCQmgKcuwvvBoJBeqwc7Kf2OQHVUXuu/FXTpNF9uuHLAO+9UWBqlyClPYJA/YaxAj1U8MfCQ/XPvErhV/zBFECYBohjTA2MnpQ3qd2exw/myQNb9HL9gL/vrFuAeeKFfvLYXyjvt63vmDMAsc8IeKjep/G0sKsgp6EgsU+ImFD09xnx96YACFMAxAza/zoAp9IRGOsKRPuwwO2ivlTpEESnSxCtqw23L7qTmz3m1TqSOyP71LlacAxcPnj0rITh6YGDpIG0TyDYeUnA3bME+xTfncV+4KqCEwLKXqN9I35AALj99QExFAYGThnqbh3LxXG7qXJs32AoKGvNYuBc/lY2yeN1Iw+c79/5VBoLCOk4va1ddX/X9zFaZHedqz+12O/xPaV9DkRS+IGpPxJMA8TRdgwM3rxjmiAmL7LL1ft955sP61c4jCu0+DuB4SzeAFcZNU/pDuzoKqSbCiF7vstNAYQpAGKO7X8dgBPqCEzrDIy32odWrefOhXnGFrilsTmLNO1Kv70ClU888k69Bk6acpTgnlMP6YAhxYgfOOSPCF2A2+8K7Lxbzlf6mjk3iwB3/xmHaGfE7S0CPMQdrjqtsEcxvnKxv2LR1wHQAWCeo38dgFONYXn6z/Y8uMJ91+lEsb2WfG2ff+/+qfal0g3Uqnycoe51tsWnw3wb1y72RvlAWAR4+X2Zg4SCPb9QdU5i5CF5v+KUb7CipUM/IB1hnYFiD5zSFECEaYA442mDXQ/pTQHcxAK+fG7/4m+pwN9CwTcFEKYAiDm2/3UAZjBtsG892aeYTQ4LZzhyvbGiboQPnIjFlDv98oe/5kfTOQSC2q8bLpLX+TXX7+02XivA6F8HwCTPybTlb2WUbUQPML0DoAswo46B0ajnELj40b8OAIcbvWYjdoCL7ADoAnCt0fA5/AKYyeh/7wAAAMywA6ALAADnP/rXAQAAHQBdAACYw+hfBwAAdAB0AQBgDqN/HQAA0AHQBQCAOYz+D9IBEAIA4LyKvykAAAhTALoAADCD0b8OAADoAOgCAMAcRv8H7wAIAQBw+sX/RqYAhAAAOO3ibw0AAIQ1ALoAADCD0f+NdgCEAAA4zeJ/41MAQgAAnF7xtwYAAMIaAF0AAJjB6P9oHQAhAABOp/gfdQpACACA0yj+R18DIAQAwO0X/1tZBCgEAMDtFv9b2wUgBADA7RV/2wABIGwD1AUAgBmM/m+9AyAEAKD4zzAACAEAKP4zDQBCAACK//GdXOF9z7Nvzf55HM9LLz70JMzYvcfvehK8/5lZ4T/ZXQC6AQAo/jPdBigEAKD4z/QcACEAAMV/pgcBCQEAKP4zPQlQCABA8Y/L3wUQdgiEVcCEXQB4/yv8rgWgGwCA4j/7iwEJAQAo/jGvKYAwJRBagIQpALz/Ff65Xw5YNwAAxX+GAUAIAEDxj/lNAYQpgdACJEwB4P2v8M89AAgCfgAgAOD9r/DHPKYAwrQAAIq/DoBugBEAOgB4/yv8AoAg4AcAAgDe/wr/nAOAIOAHAAIA3v8K/4wDgCDgBwACAN7/cy78sw8AgoAfAAgAAsBDhX/GrJafeRAQAAQABACFXwBghkFAABAAEAAUfgGAGYYBAUAAQABQ9AUAZhgEBAABAAFA4RcAmGEYEAAEAAQARV8AYIZhQAAQABAAFH0BgBkGAgFAAEAAUPAFAGYYCAQAAQABQMEXAJhhKBAABAAEAMVeAGCGAUEAEAAQABT4efr/dCZljg29InIAAAAASUVORK5CYII=', 'base64');
const ICON_192 = Buffer.from('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', 'base64');
app.get('/icon-512.png', (req, res) => res.type('png').set('Cache-Control', 'public, max-age=604800').send(ICON_512));
app.get('/icon-192.png', (req, res) => res.type('png').set('Cache-Control', 'public, max-age=604800').send(ICON_192));
app.get('/manifest.webmanifest', (req, res) => res.type('application/manifest+json').send(JSON.stringify({
  name: "Rene's Minigolf World", short_name: 'Minigolf', start_url: '/', display: 'standalone', orientation: 'any',
  background_color: '#13171d', theme_color: '#1f2329', lang: 'de',
  icons: [{ src: '/icon-192.png', sizes: '192x192', type: 'image/png' }, { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any maskable' }],
})));

app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size, redis: !!redis }));

app.get('/api/meta', (req, res) => {
  const date = viennaDate();
  res.json({ holes: HOLES, tours: Object.fromEntries(Object.keys(TOURS).map((t) => [t, { holes: TOURS[t], sig: tourSig(t) }])), daily: { date, hole: dailyHole(date) }, now: Date.now() });
});

app.post('/api/score', async (req, res) => {
  if (limited(req.ip)) return res.status(429).json({ error: 'Zu viele Einträge, kurz warten.' });
  const { hole, ver, strokes, ms, daily } = req.body || {};
  const name = cleanName(req.body?.name);
  const h = HOLE[hole];
  if (!h || name.length < 2) return res.status(400).json({ error: 'Ungültig' });
  if (ver !== h.ver) return res.status(409).json({ error: 'Diese Bahn wurde geändert – bitte Seite neu laden.' });
  const s = Math.round(strokes), t = Math.round(ms);
  if (!(s >= 1 && s <= MAX_SCORE) || !(t >= MIN_MS && t < 3600000)) return res.status(400).json({ error: 'Ungültiges Ergebnis' });
  const out = {};
  const put = async (key) => {
    const b = await load(key, () => ({ entries: {} }));
    const k = lc(name), old = b.entries[k];
    const e = { name, strokes: s, ms: t, at: Date.now() };
    let improved = false;
    if (!old || cmpEntry(e, old) < 0) { b.entries[k] = e; improved = true; save(key); }
    const list = Object.values(b.entries).sort(cmpEntry);
    return { improved, rank: list.findIndex((x) => lc(x.name) === k) + 1, total: list.length, best: b.entries[k] };
  };
  out.hole = await put(boardKey('hole', hole));
  if (daily) {
    const date = viennaDate();
    if (dailyHole(date) === hole) out.daily = await put(boardKey('daily', date));
  }
  res.json(out);
});

app.post('/api/tour', async (req, res) => {
  if (limited(req.ip)) return res.status(429).json({ error: 'Zu viele Einträge, kurz warten.' });
  const { tour, sig, splits, ms } = req.body || {};
  const name = cleanName(req.body?.name);
  if (!TOURS[tour] || name.length < 2) return res.status(400).json({ error: 'Ungültig' });
  if (sig !== tourSig(tour)) return res.status(409).json({ error: 'Die Tour wurde geändert – bitte Seite neu laden.' });
  if (!Array.isArray(splits) || splits.length !== TOURS[tour].length) return res.status(400).json({ error: 'Ungültig' });
  const sp = splits.map((x) => Math.round(x));
  if (sp.some((x) => !(x >= 1 && x <= MAX_SCORE))) return res.status(400).json({ error: 'Ungültig' });
  const t = Math.round(ms);
  if (!(t >= MIN_MS * sp.length && t < 4 * 3600000)) return res.status(400).json({ error: 'Ungültig' });
  const key = boardKey('tour', tour);
  const b = await load(key, () => ({ entries: {} }));
  const k = lc(name), old = b.entries[k];
  const e = { name, strokes: sp.reduce((a, x) => a + x, 0), ms: t, splits: sp, at: Date.now() };
  let improved = false;
  if (!old || cmpEntry(e, old) < 0) { b.entries[k] = e; improved = true; save(key); }
  const list = Object.values(b.entries).sort(cmpEntry);
  res.json({ improved, rank: list.findIndex((x) => lc(x.name) === k) + 1, total: list.length, best: b.entries[k] });
});

app.get('/api/board', async (req, res) => {
  const kind = String(req.query.kind || 'hole');
  const id = String(req.query.id || (kind === 'daily' ? viennaDate() : ''));
  const key = boardKey(kind, id);
  if (!key) return res.status(400).json({ error: 'Unbekannte Liste' });
  const b = await load(key, () => ({ entries: {} }));
  let list = Object.values(b.entries);
  if (kind === 'mp') list.sort((a, c) => c.wins - a.wins || (a.strokes / a.holes) - (c.strokes / c.holes) || c.games - a.games);
  else list.sort(cmpEntry);
  const members = await groupMembers(req.query.group ? String(req.query.group) : '');
  if (members) list = list.filter((e) => members.has(lc(e.name)));
  const me = lc(req.query.name || '');
  const myRank = me ? list.findIndex((e) => lc(e.name) === me) + 1 : 0;
  const limit = Math.min(200, Math.max(1, parseInt(req.query.limit, 10) || 50));
  res.json({ list: list.slice(0, limit), total: list.length, me: myRank ? { rank: myRank, entry: list[myRank - 1] } : null, hole: kind === 'daily' ? dailyHole(id) : undefined });
});

// Meine Bestwerte auf einen Blick (für das Menü)
app.get('/api/mine', async (req, res) => {
  const me = lc(req.query.name || '');
  const out = {};
  if (!me) return res.json(out);
  for (const h of HOLES) { const b = await load(boardKey('hole', h.id), () => ({ entries: {} })); if (b.entries[me]) out[h.id] = b.entries[me]; }
  for (const t of Object.keys(TOURS)) { const b = await load(boardKey('tour', t), () => ({ entries: {} })); if (b.entries[me]) out[t] = b.entries[me]; }
  res.json(out);
});

// ---------- Gruppen
app.get('/api/group', async (req, res) => {
  const g = String(req.query.g || '').toLowerCase().trim();
  const groups = await load('groups', () => ({}));
  const gr = groups[g];
  res.json(gr ? { name: gr.name, members: Object.values(gr.members) } : { name: req.query.g, members: [] });
});
app.post('/api/group/join', async (req, res) => {
  if (limited(req.ip)) return res.status(429).json({ error: 'Kurz warten.' });
  const name = cleanName(req.body?.name);
  const gname = cleanName(req.body?.group);
  if (name.length < 2 || gname.length < 2) return res.status(400).json({ error: 'Name und Gruppe brauchen mindestens 2 Zeichen.' });
  const groups = await load('groups', () => ({}));
  const g = gname.toLowerCase();
  if (!groups[g]) groups[g] = { name: gname, members: {}, at: Date.now() };
  if (Object.keys(groups[g].members).length >= 200) return res.status(400).json({ error: 'Gruppe ist voll.' });
  groups[g].members[lc(name)] = name;
  save('groups');
  res.json({ name: groups[g].name, members: Object.values(groups[g].members) });
});
app.post('/api/group/leave', async (req, res) => {
  const name = cleanName(req.body?.name);
  const g = String(req.body?.group || '').toLowerCase().trim();
  const groups = await load('groups', () => ({}));
  if (groups[g]) { delete groups[g].members[lc(name)]; if (!Object.keys(groups[g].members).length) delete groups[g]; save('groups'); }
  res.json({ ok: true });
});

// ---------- Feedback-Pinnwand
app.get('/api/feedback', async (req, res) => {
  const fb = await load('feedback', () => ({ posts: [] }));
  res.json({ posts: fb.posts.slice(-150).reverse().map(({ token, likers, ...p }) => ({ ...p, likes: (likers || []).length })) });
});
app.get('/api/feedback/count', async (req, res) => {
  const fb = await load('feedback', () => ({ posts: [] }));
  const since = Number(req.query.since) || 0;
  res.json({ count: fb.posts.filter((p) => p.at > since).length });
});
app.post('/api/feedback', async (req, res) => {
  if (limited(req.ip, 6)) return res.status(429).json({ error: 'Bitte kurz warten.' });
  const name = cleanName(req.body?.name);
  const text = String(req.body?.text || '').replace(/\s+/g, ' ').trim().slice(0, 600);
  if (name.length < 2 || text.length < 3) return res.status(400).json({ error: 'Bitte etwas mehr schreiben.' });
  const fb = await load('feedback', () => ({ posts: [] }));
  const token = crypto.randomBytes(12).toString('hex');
  const post = { id: crypto.randomBytes(6).toString('hex'), name, text, at: Date.now(), status: 'neu', reply: '', token, likers: [] };
  fb.posts.push(post);
  if (fb.posts.length > 500) fb.posts.splice(0, fb.posts.length - 500);
  save('feedback');
  res.json({ id: post.id, token });
});
app.post('/api/feedback/like', async (req, res) => {
  const fb = await load('feedback', () => ({ posts: [] }));
  const p = fb.posts.find((x) => x.id === req.body?.id);
  const who = lc(req.body?.name);
  if (!p || !who) return res.status(404).json({ error: 'Nicht gefunden' });
  p.likers = p.likers || [];
  if (p.likers.includes(who)) p.likers = p.likers.filter((x) => x !== who); else p.likers.push(who);
  save('feedback');
  res.json({ likes: p.likers.length });
});
app.post('/api/feedback/delete', async (req, res) => {
  const fb = await load('feedback', () => ({ posts: [] }));
  const i = fb.posts.findIndex((x) => x.id === req.body?.id && x.token === req.body?.token);
  if (i < 0) return res.status(403).json({ error: 'Nur eigene Beiträge.' });
  fb.posts.splice(i, 1);
  save('feedback');
  res.json({ ok: true });
});
app.post('/api/feedback/admin', async (req, res) => {
  if (!ADMIN_KEY || req.body?.key !== ADMIN_KEY) return res.status(403).json({ error: 'Falsches Passwort' });
  const fb = await load('feedback', () => ({ posts: [] }));
  const { action, id, text, status } = req.body;
  if (action === 'check') return res.json({ ok: true });
  const i = fb.posts.findIndex((x) => x.id === id);
  if (i < 0) return res.status(404).json({ error: 'Nicht gefunden' });
  if (action === 'delete') fb.posts.splice(i, 1);
  else if (action === 'reply') fb.posts[i].reply = String(text || '').slice(0, 600);
  else if (action === 'status') fb.posts[i].status = ['neu', 'geplant', 'erledigt', 'abgelehnt'].includes(status) ? status : 'neu';
  save('feedback');
  res.json({ ok: true });
});
app.post('/api/admin/reset', async (req, res) => {
  if (!ADMIN_KEY || req.body?.key !== ADMIN_KEY) return res.status(403).json({ error: 'Falsches Passwort' });
  const key = boardKey(req.body.kind, req.body.id);
  if (!key) return res.status(400).json({ error: 'Unbekannte Liste' });
  const b = await load(key, () => ({ entries: {} }));
  // Sicherung behalten
  const bk = key + ':sicherung:' + viennaDate();
  cache.set(bk, JSON.parse(JSON.stringify(b))); save(bk);
  b.entries = {}; save(key);
  res.json({ ok: true, backup: bk });
});

// ---------------------------------------------------------------- Mehrspieler
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
const rooms = new Map();
const MAX_PLAYERS = 6;
const COLORS = ['weiss', 'rot', 'blau', 'gelb', 'gruen', 'pink', 'orange', 'lila', 'tuerkis'];
const BETWEEN_MS = 7000, START_DELAY = 4500, WAIT_LAST_MS = 75000, DISC_GRACE = 25000;

const cleanCode = (c) => String(c || '').toUpperCase().replace(/[^A-Z0-9ÄÖÜ-]/g, '').slice(0, 12);
function planHoles(plan) {
  if (!plan) return TOURS.tour1;
  if (plan.kind === 'hole' && HOLE[plan.hole]) return [plan.hole];
  if (plan.kind === 'all') return HOLES.map((h) => h.id);
  if (TOURS[plan.kind]) return TOURS[plan.kind];
  return TOURS.tour1;
}
function snapshot(r) {
  return {
    code: r.code, phase: r.phase, plan: r.plan, holes: r.holes, holeIdx: r.holeIdx, holeStartAt: r.holeStartAt,
    deadline: r.deadline, betweenUntil: r.betweenUntil, hostUid: r.hostUid, serverNow: Date.now(),
    players: [...r.players.values()].map((p) => ({
      uid: p.uid, name: p.name, color: p.color, connected: p.connected, scores: p.scores,
      cur: { n: p.cur.n, done: p.cur.done, st: p.cur.st }, total: p.scores.reduce((a, x) => a + (x || 0), 0),
    })),
  };
}
function send(ws, msg) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); }
function broadcast(r, msg, except) { const s = JSON.stringify(msg); for (const p of r.players.values()) if (p.ws && p.ws !== except && p.ws.readyState === 1) p.ws.send(s); }
function pushRoom(r) { r.lastActive = Date.now(); broadcast(r, { t: 'room', room: snapshot(r) }); }
function freeColor(r, want, uid) {
  const used = new Set([...r.players.values()].filter((p) => p.uid !== uid).map((p) => p.color));
  if (COLORS.includes(want) && !used.has(want)) return want;
  return COLORS.find((c) => !used.has(c)) || 'weiss';
}
function uniqueName(r, name, uid) {
  let n = name, k = 2;
  while ([...r.players.values()].some((p) => p.uid !== uid && p.name.toLowerCase() === n.toLowerCase())) n = name.slice(0, 13) + ' ' + k++;
  return n;
}
function startHole(r, idx) {
  r.phase = 'play';
  r.holeIdx = idx;
  r.holeStartAt = Date.now() + START_DELAY;
  r.deadline = null;
  r.betweenUntil = null;
  for (const p of r.players.values()) {
    p.cur = { n: 0, done: false, st: 'aim' };
    while (p.scores.length < idx) p.scores.push(MAX_SCORE);
    if (!p.connected && Date.now() - (p.discAt || 0) > DISC_GRACE) { p.cur = { n: MAX_SCORE, done: true, st: 'weg' }; p.scores[idx] = MAX_SCORE; }
  }
  pushRoom(r);
}
async function finishGame(r) {
  r.phase = 'end';
  r.betweenUntil = null;
  pushRoom(r);
  const ps = [...r.players.values()];
  if (ps.length < 2 || r.recorded) return;
  r.recorded = true;
  const totals = ps.map((p) => ({ p, total: p.scores.reduce((a, x) => a + (x || MAX_SCORE), 0) }));
  const best = Math.min(...totals.map((x) => x.total));
  const b = await load('board:mp', () => ({ entries: {} }));
  for (const { p, total } of totals) {
    const k = lc(p.name);
    if (!k) continue;
    const e = b.entries[k] || { name: cleanName(p.name), wins: 0, games: 0, strokes: 0, holes: 0, at: Date.now() };
    e.games++; e.strokes += total; e.holes += r.holes.length; e.at = Date.now();
    if (total === best) e.wins++;
    b.entries[k] = e;
  }
  save('board:mp');
}
function checkHoleDone(r) {
  if (r.phase !== 'play') return;
  const ps = [...r.players.values()];
  const active = ps.filter((p) => p.connected || Date.now() - (p.discAt || 0) < DISC_GRACE);
  const allDone = active.every((p) => p.cur.done);
  if (allDone) {
    for (const p of ps) if (!p.cur.done) { p.cur.done = true; p.cur.st = 'weg'; p.scores[r.holeIdx] = MAX_SCORE; }
    r.phase = 'between';
    r.betweenUntil = Date.now() + BETWEEN_MS;
    r.deadline = null;
    pushRoom(r);
    return;
  }
  if (!r.deadline && ps.some((p) => p.cur.done)) { r.deadline = Date.now() + WAIT_LAST_MS; pushRoom(r); }
}
setInterval(() => {
  const now = Date.now();
  for (const [code, r] of rooms) {
    const anyConnected = [...r.players.values()].some((p) => p.connected);
    if (!anyConnected && now - r.lastActive > 15 * 60000) { rooms.delete(code); continue; }
    if (r.phase === 'play') {
      if (r.deadline && now > r.deadline) {
        for (const p of r.players.values()) if (!p.cur.done) { p.cur.done = true; p.cur.st = 'zeit'; p.scores[r.holeIdx] = MAX_SCORE; }
      }
      checkHoleDone(r);
    } else if (r.phase === 'between' && now > r.betweenUntil) {
      if (r.holeIdx + 1 < r.holes.length) startHole(r, r.holeIdx + 1);
      else finishGame(r);
    }
  }
}, 400);

wss.on('connection', (ws, req) => {
  let me = null, room = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data) => {
    let m;
    try { m = JSON.parse(data); } catch { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'ping') return send(ws, { t: 'pong', c: m.c, s: Date.now() });
    if (m.t === 'create' || m.t === 'join') {
      const code = cleanCode(m.code);
      const uid = String(m.uid || '').slice(0, 40);
      const name = cleanName(m.name);
      if (code.length < 3) return send(ws, { t: 'err', msg: 'Der Code braucht mindestens 3 Zeichen.' });
      if (!uid || name.length < 2) return send(ws, { t: 'err', msg: 'Bitte zuerst einen Namen wählen.' });
      let r = rooms.get(code);
      if (m.t === 'create') {
        if (r && [...r.players.values()].some((p) => p.connected && p.uid !== uid)) return send(ws, { t: 'err', msg: `Raum „${code}“ gibt es schon – beitreten oder anderen Code wählen.` });
        if (!r || ![...r.players.values()].some((p) => p.uid === uid)) {
          r = { code, created: Date.now(), hostUid: uid, plan: m.plan || { kind: 'tour1' }, holes: planHoles(m.plan), phase: 'lobby', holeIdx: 0, players: new Map(), lastActive: Date.now() };
          rooms.set(code, r);
        }
      } else if (!r) return send(ws, { t: 'err', msg: `Kein Raum mit Code „${code}“ gefunden.` });
      let p = r.players.get(uid);
      if (!p) {
        const live = [...r.players.values()].filter((x) => x.connected || Date.now() - (x.discAt || 0) < DISC_GRACE).length;
        if (live >= MAX_PLAYERS) return send(ws, { t: 'err', msg: 'Der Raum ist voll (6 Spieler).' });
        p = { uid, name: uniqueName(r, name, uid), color: freeColor(r, m.color, uid), scores: [], cur: { n: 0, done: false, st: 'aim' }, joinedAt: Date.now() };
        if (r.phase === 'play' || r.phase === 'between') {
          while (p.scores.length < r.holeIdx + (r.phase === 'between' ? 1 : 0)) p.scores.push(MAX_SCORE);
          if (r.phase === 'between') p.cur = { n: MAX_SCORE, done: true, st: 'spaet' };
        }
        r.players.set(uid, p);
      } else if (p.ws && p.ws !== ws) { try { p.ws.close(4000, 'neu verbunden'); } catch { /* egal */ } }
      p.ws = ws; p.connected = true; p.discAt = null;
      if (!r.players.has(r.hostUid) || !r.players.get(r.hostUid).connected) r.hostUid = uid;
      me = p; room = r;
      send(ws, { t: 'you', uid, code });
      pushRoom(r);
      if (p.lastPos) send(ws, { t: 'pos', uid, x: p.lastPos.x, y: p.lastPos.y, m: 0 });
      for (const o of r.players.values()) if (o !== p && o.lastPos && r.phase === 'play') send(ws, { t: 'pos', uid: o.uid, x: o.lastPos.x, y: o.lastPos.y, m: 0 });
      return;
    }
    if (!me || !room) return;
    const r = room;
    switch (m.t) {
      case 'color': me.color = freeColor(r, m.color, me.uid); pushRoom(r); break;
      case 'plan':
        if (me.uid !== r.hostUid || (r.phase !== 'lobby' && r.phase !== 'end')) break;
        r.plan = m.plan; r.holes = planHoles(m.plan); pushRoom(r); break;
      case 'start':
        if (me.uid !== r.hostUid || (r.phase !== 'lobby' && r.phase !== 'end')) break;
        r.recorded = false;
        for (const p of r.players.values()) { p.scores = []; p.lastPos = null; }
        // Getrennte Spieler fliegen bei Neustart raus
        for (const [u, p] of r.players) if (!p.connected) r.players.delete(u);
        startHole(r, 0);
        break;
      case 'shot':
        if (r.phase !== 'play' || me.cur.done || m.hole !== r.holeIdx) break;
        me.cur.n = Math.min(MAX_SCORE, Math.round(m.n) || me.cur.n + 1);
        me.cur.st = 'roll';
        broadcast(r, { t: 'shot', uid: me.uid, x: +m.x, y: +m.y, n: me.cur.n }, ws);
        break;
      case 'pos':
        if (r.phase !== 'play') break;
        me.lastPos = { x: +m.x, y: +m.y };
        broadcast(r, { t: 'pos', uid: me.uid, x: +m.x, y: +m.y, m: m.m ? 1 : 0, ts: Math.abs((+m.ts || 0) - Date.now()) < 5000 ? +m.ts : Date.now() }, ws);
        break;
      case 'rest': {
        if (r.phase !== 'play' || me.cur.done || m.hole !== r.holeIdx) break;
        const n = Math.max(1, Math.min(MAX_SCORE, Math.round(m.n) || 1));
        me.cur.n = n;
        me.lastPos = { x: +m.x, y: +m.y };
        if (m.st === 'cup') { me.cur.done = true; me.cur.st = 'cup'; me.scores[r.holeIdx] = n; }
        else if (n >= MAX_SCORE - 1) { me.cur.done = true; me.cur.st = 'max'; me.scores[r.holeIdx] = MAX_SCORE; }
        else me.cur.st = 'aim';
        broadcast(r, { t: 'rest', uid: me.uid, x: +m.x, y: +m.y, st: m.st, n, ts: Date.now() }, ws);
        pushRoom(r);
        checkHoleDone(r);
        break;
      }
      case 'giveup':
        if (r.phase !== 'play' || me.cur.done) break;
        me.cur.done = true; me.cur.st = 'max'; me.scores[r.holeIdx] = MAX_SCORE;
        pushRoom(r); checkHoleDone(r);
        break;
      case 'emote': {
        const e = String(m.e || '').slice(0, 8);
        broadcast(r, { t: 'emote', uid: me.uid, e });
        break;
      }
      case 'skip':
        if (me.uid !== r.hostUid) break;
        if (r.phase === 'play') { r.deadline = Date.now(); }
        else if (r.phase === 'between') r.betweenUntil = Date.now();
        break;
      case 'leave':
        r.players.delete(me.uid);
        if (r.hostUid === me.uid) { const n = [...r.players.values()].find((p) => p.connected); if (n) r.hostUid = n.uid; }
        if (!r.players.size) rooms.delete(r.code); else { pushRoom(r); checkHoleDone(r); }
        me = null; room = null;
        break;
    }
  });
  ws.on('close', () => {
    if (!me || !room) return;
    if (me.ws !== ws) return;
    me.connected = false; me.discAt = Date.now(); me.ws = null;
    if (room.hostUid === me.uid) { const n = [...room.players.values()].find((p) => p.connected); if (n) room.hostUid = n.uid; }
    pushRoom(room);
  });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch { /* egal */ } } }, 25000);

await initRedis();
server.listen(PORT, () => console.log(`Rene's Minigolf World läuft auf Port ${PORT}`));
