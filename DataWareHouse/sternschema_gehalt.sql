SET foreign_key_checks = 0;

-- Zeitdimension
DROP TABLE IF EXISTS dim_zeit;

CREATE TABLE dim_zeit (
  zeit_id INT AUTO_INCREMENT PRIMARY KEY,
  datum DATE NOT NULL,
  jahr SMALLINT,
  monat TINYINT,
  quartal TINYINT,
  wochentag_name VARCHAR(10)
);

-- Mitarbeiterdimension
DROP TABLE IF EXISTS dim_mitarbeiter;

CREATE TABLE dim_mitarbeiter (
  mitarbeiter_id INT AUTO_INCREMENT PRIMARY KEY
  -- hier bitte die Attributdefinitionen einsetzen
);

-- Faktentabelle: Gehaltszahlung
DROP TABLE IF EXISTS fact_gehalt;

CREATE TABLE fact_gehalt (
  fact_id INT AUTO_INCREMENT PRIMARY KEY,
  mitarbeiter_id INT NOT NULL,
  zeit_id INT NOT NULL,
  betrag INT NOT NULL,
  bemerkung VARCHAR(100),
  FOREIGN KEY (mitarbeiter_id) REFERENCES dim_mitarbeiter(mitarbeiter_id),
  FOREIGN KEY (zeit_id) REFERENCES dim_zeit(zeit_id)
);

SET foreign_key_checks = 1;