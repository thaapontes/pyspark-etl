CREATE TABLE CARS(
    Name NVARCHAR(70) NOT NULL,
    MilesPerGallon INT,
    Cylinders INT,
    Displacement INT,
    Horsepower INT,
    WeightInLbs INT,
    Acceleration NUMERIC(3, 1),
    YearDate NVARCHAR(12),
    Origin NVARCHAR(12),
    Price NUMERIC(15, 2),
    CONSTRAINT PK_CARS_Name PRIMARY KEY ( Name )
);

CREATE VIEW vw_WithPriceRange AS
SELECT c.*,
    CASE
        WHEN Price < 10000 THEN "low"
        WHEN Price > 10000 && Price < 30000 THEN "medium"
        ELSE "high"
    END AS PriceRange
FROM CARS c;

CREATE VIEW vw_CountPriceRangePerManufacturer AS
SELECT Manufacturer, PriceRange, COUNT(*)
FROM vw_WithPriceRange
GROUP BY Manufacturer, PriceRange;