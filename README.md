

plan: 

läsa fil
filtrera på gbg, skicka till json 
använd json och bing search för att hitta hemsidor
lokalisera hesmidor leta mail
spara i json fil 
sortera företagsnamn och mail 
mvp ovan 

sen få ner allt snyggt och strukturerat så man kan markera eller liknande för att se vilka man skickat mial till

automatiskt script för att skicka mail ? 

fråga peder efter mail som vi skickar ut och koppla det så man skickar mail 

dubbelkolla så det är b2b bara så inte didup får skit
klar ? 

SNI filtering , kan filktrera på arkitetker typ ?? värt eller inte ? perosnligare mail 



TODO

Skapa en masterlista för Göteborg i SQLite
Skapa en SQLite-databas med en tabell companies där varje rad är ett företag
Fält ska minst vara orgnr, namn, stad, website, emails, sni_codes, employees, website_checked_at, emails_checked_at, last_seen_at
Importera din nuvarande NDJSON-lista till databasen
Denna databas är den enda sanningskällan framåt

Bygg ett sync-script för att uppdatera företagslistan
Scriptet läser en ny GBG-lista när du får den
För varje orgnr görs upsert (update om finns, insert annars)
Sätt last_seen_at = nu för alla som finns i senaste listan
Detta gör att databasen alltid är uppdaterad utan att tappa historik

Inför SNI-berikning som grund för all filtrering
Hämta SNI-kod(er) och branschtext gratis via SCB
Spara SNI 2025-koder i databasen
Sluta filtrera bort “dåliga” företag via namn
All prioritering ska ske via SNI-grupper

Definiera segment baserat på SNI och företagsdata
Top prospects = SNI inom kontor/kunskap/IT/bygg/projekt AND employees >= 1 AND website finns
Mid prospects = relevanta SNI men okänd storlek eller svagare signaler
Low prospects = SNI som du inte vill jobba med (pizzeria, frisör etc)
Segmenten är queries mot databasen, inte separata listor

Porta website-finder till databasen med refresh-logik
När website_checked_at saknas ska företaget köras
När website_checked_at är äldre än 90 dagar ska företaget köras igen
Annars ska företaget hoppas över
Resultatet sparas direkt i databasen
Detta gör att du kan köra scriptet när som helst utan att göra om arbete

Porta email-finder till databasen med refresh-logik
När emails_checked_at saknas ska företaget köras
När emails_checked_at är äldre än 180 dagar ska företaget köras igen
Annars hoppas företaget över
Emails sparas i databasen (gärna JSON)
Valfritt: filtrera bort gmail/hotmail för B2B-fokus

Exportera segment till Excel eller CSV vid behov
Skapa ett export-script där du väljer ett segment (t.ex. Top prospects)
Exportera orgnr, namn, website, emails, SNI-text
En export = ett outreach-case
Ingen data dupliceras, allt kommer från master

Bygg en mail-generator per segment eller SNI
Mail-generatorn tar företagsnamn, bransch (SNI-text) och ev. hemsidans title/meta
Den skapar ämnesrad A/B
Den skapar 2–3 pitch-varianter
Den lägger in 1–2 meningar som är bransch-anpassade
Allt sparas som drafts i CSV eller Excel

Bygg ett separat mailer-script
Mailer-scriptet läser CSV/Excel med email, company, subject, body
Skickar via SMTP, SendGrid eller Microsoft Graph
Loggar sent_at, status, bounce och svar
Detta script är helt frikopplat från datainsamlingen

ett script som går igneom db och kollar dem som inte vi hiitade sni på kollar bar apå dem och letar igen
ett script som lägger in started at på dem i göteborg det har jag inte värderna på 