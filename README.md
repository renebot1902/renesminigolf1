# Rene's Minigolf World

3D-Minigolf im Browser mit Bahnen aus Graz und der Steiermark. Man spielt allein, als Tour oder mit bis zu 6 Spielern gleichzeitig, jeder auf seinem Handy oder PC.

## Dateien im Repo

| Datei | Inhalt |
|---|---|
| `server.js` | Server: liefert das Spiel aus, speichert Ranglisten, Gruppen und die Feedback-Pinnwand, führt die Mehrspieler-Räume (WebSocket `/ws`) |
| `package.json` | Node-Pakete (express, ws, redis, compression) |
| `index.html` | das ganze Spiel in einer Datei (wird aus dem Quellcode gebaut, nie von Hand bearbeiten); darf auch im Ordner `public` liegen |
| `README.md` | diese Anleitung |

## Betrieb auf Railway

- GitHub-Repo: `renebot1902/renesminigolf1` (Zweig `main`)
- Railway-Projekt: **rene-minigolf** (eigenes Projekt, unabhängig von Kart World und den anderen Projekten)
  - Dienst `minigolf`: baut aus dem Repo, Adresse `https://minigolf-production-0ef3.up.railway.app`
  - Dienst `Redis`: eigene Datenbank nur für Minigolf
- Variablen im Dienst `minigolf`:
  - `REDIS_URL` = `${{Redis.REDIS_URL}}`
  - `ADMIN_KEY` = eigenes Passwort für die Pinnwand-Verwaltung (nur in Railway, nie in eine Datei schreiben)

Ohne `REDIS_URL` läuft das Spiel auch, vergisst aber Ranglisten und Pinnwand bei jedem Neustart.

## Als App installieren

Das Spiel ist eine installierbare Web-App (PWA), kein App-Store nötig. Auf der Startseite gibt es den grünen Knopf „Als App installieren“:

- **Android (Chrome):** Der Knopf öffnet direkt den Installieren-Dialog. Kommt man aus WhatsApp, zuerst oben rechts ⋮ → „In Chrome öffnen“.
- **iPhone (Safari):** Teilen → „Zum Home-Bildschirm“.
- **PC (Chrome/Edge):** Installieren-Symbol in der Adresszeile.

Danach hat Minigolf ein eigenes Symbol und startet im Vollbild. Updates kommen automatisch, sobald eine neue `index.html` im Repo ist. Manifest, App-Symbole und Service Worker liefert `server.js` mit (`/manifest.webmanifest`, `/icon-192.png`, `/icon-512.png`, `/icon-maskable.png`, `/sw.js`).

## Grafik und Handy

- Handys starten mit Grafikstufe „mittel“, PCs mit „hoch“. Ruckelt es länger als ein paar Sekunden, stellt das Spiel automatisch eine Stufe tiefer (außer man hat die Stufe in den Einstellungen selbst gewählt).
- Im Hochformat schaut die Kamera steiler von oben, damit man die Bahn vor sich sieht.
- Für volle Schlagkraft muss man weit ziehen, damit man am Handy fein dosieren kann.

## Neue Version hochladen

1. Zuerst `server.js` im Hauptverzeichnis hochladen → Commit (falls geliefert).
2. Danach die neue `index.html` ins Hauptverzeichnis hochladen → Commit. Die Datei muss auf dem Rechner genau `index.html` heißen (`index (1).html` vorher umbenennen).
3. Nie im GitHub-Editor bearbeiten oder umbenennen.
4. Railway baut nach jedem Commit selbst neu. Offene Mehrspieler-Räume gehen dabei verloren, also nicht hochladen, während gerade jemand spielt.

## Bahnen

| Nr | Kennung | Name | Teil | Par | Besonderheit |
|---|---|---|---|---|---|
| 1 | `hauptplatz` | Hauptplatz | 1 Graz | 2 | Brunnen in der Mitte, Standl, Rathaus |
| 2 | `murinsel` | Murinsel | 1 Graz | 3 | zwei schmale Stege über die Mur, Wasser = +1 |
| 3 | `schlossberg` | Uhrturm | 1 Graz | 3 | Rampe, Ziffernblatt mit drehenden Zeigern (großer Zeiger = Stunden wie am Original), Schlossberg-Stollen als Abkürzung |
| 4 | `kunsthaus` | Kunsthaus | 1 Graz | 3 | Flipper mit Düsen, die zurückschießen, Turbo-Pfeile |
| 5 | `erzberg` | Erzberg | 2 Steiermark | 3 | drei Etagen bergab, Muldenkipper fährt quer |
| 6 | `klapotetz` | Klapotetz | 2 Steiermark | 3 | Windrad über dem Durchgang, Kürbisse, Weinreben |
| 7 | `dachstein` | Dachstein | 2 Steiermark | 3 | Eis (rutschig), Schnee (bremst), Hängebrücke über die Gletscherspalte |
| 8 | `riegersburg` | Riegersburg | 2 Steiermark | 3 | Zugbrücke geht auf und zu, Burggraben, Ritter im Hof |

Touren: `tour1` Graz-Tour (Bahnen 1–4), `tour2` Steiermark-Tour (Bahnen 5–8). Neue Bahnen kommen immer ganz hinten dazu, je 4 Bahnen ergeben automatisch eine neue Tour.

## Regeln

- Wenigste Schläge gewinnen, bei Gleichstand zählt die Zeit.
- Wasser, Gletscherspalte oder Aus = 1 Strafschlag, der Ball kommt zurück an die letzte Stelle.
- Nach 7 Schlägen ohne Loch zählt die Bahn 8.
- Mehrspieler: Alle spielen gleichzeitig, die Bälle rollen durcheinander durch. Sobald der Erste fertig ist, haben die anderen 75 Sekunden. Wer dann noch nicht drin ist, bekommt 8.
- Mehrspieler-Ergebnisse zählen in einer eigenen Rangliste (Siege, Spiele, Schnitt pro Bahn), nicht in den Bahn-Ranglisten.

## Schnittstellen

| Schnittstelle | Zweck |
|---|---|
| `GET /api/meta` | Bahnen, Touren mit Signatur, Bahn des Tages |
| `POST /api/score` `{name, hole, ver, strokes, ms, daily}` | Bahn-Ergebnis eintragen |
| `POST /api/tour` `{name, tour, sig, splits, ms}` | Tour-Ergebnis eintragen |
| `GET /api/board?kind=hole\|tour\|daily\|mp&id=&group=&name=` | Rangliste |
| `GET /api/mine?name=` | eigene Bestwerte |
| `GET /api/group?g=`, `POST /api/group/join`, `POST /api/group/leave` | Gruppen |
| `GET/POST /api/feedback`, `/like`, `/delete`, `/admin` | Pinnwand |
| `POST /api/admin/reset` `{key, kind, id}` | eine Rangliste leeren (Sicherung bleibt) |
| WebSocket `/ws` | Mehrspieler-Räume (Code 3–12 Zeichen, max. 6 Spieler, Einladungslink `?raum=CODE`) |
| `GET /health` | Statusprüfung |

Redis-Schlüssel: `minigolf:board:hole:<bahn>:v<version>`, `minigolf:board:tour:<tour>:<signatur>`, `minigolf:board:daily:<datum>`, `minigolf:board:mp`, `minigolf:groups`, `minigolf:feedback`.

Wird eine Bahn so umgebaut, dass alte Ergebnisse nicht mehr vergleichbar sind, wird ihre Version im Spiel (`HOLE_VER`) und im Server (`HOLES`) gemeinsam erhöht. Dann beginnt ihre Rangliste frisch, die alte bleibt in Redis liegen.

## Weiterbauen

Der Quellcode liegt nicht im Repo, sondern im Zip `minigolf-quelle.zip`:

- `src/holes.js`: Bahnen
- `src/physics.js`: Ballphysik
- `src/render/`: 3D-Grafik
- `src/app.js`: Menüs
- `server.js`: Server

Gebaut wird mit `npm install` und `node tools/build.mjs game`, das schreibt `public/index.html` (für das Repo kommt sie ins Hauptverzeichnis). Mit `node tools/bot.mjs` prüft ein Bot, ob alle Bahnen lösbar sind und das Par passt.
