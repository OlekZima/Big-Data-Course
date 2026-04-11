#set page(paper: "a4", numbering: "1 z 1")
#set text(size: 15pt, hyphenate: true, lang: "pl")
#set document(title: [Problemy powiązane z Big Data])

#image("PJATK_PL_poziom_1.png")

#align(center, title())

= Problemy Biznesowe

== Koszt przechowywania danych
Żadna firma nie chce stracić dane, na których operuje na codzień;
korzystają z różnego rodzaju backupów i redundantnych kopii zapasowych (np. usługi chmurowe czy systemy `RAID`).
Każde z takich zastosowań ma koszt, który musi być uwzględniony w budżecie firmy.
Może być to koszt bardziej _jednorazowy_ lub _okresowy_, w zależności od wybranego podejścia.

== Lokalizacja danych
W przypadku rozwiązań chmurowych koszty mogą się różnić w zależności od lokalizacji serwerów.
Dodatkowo należy pamiętać o prawach obowiązujących w danych regionach, które mogą wpływać na dostępność danych lub na działania, które firma musi podjąć.

== Jakość danych
Jakość danych wprost wpływa na jakość analiz i analitycznych decyzji, które mogą doprowadzić, w przypadku danych o niskiej jakości, do wielomilionowych strat.
W danych o niskiej jakości mogą występować m.in. zduplikowane rekordy, brakujące wartości, niespójne formatowanie i przestarzałe informacje.

= Problemy Techniczne

== Dostęp do danych
W wielu firmach brakuje stanowiska odpowiedzialnego za obsługę i dostęp do danych.
Często dane muszą być anonimizowane i przechowywane w sposób chroniony przed nieautoryzowanym dostępem.

== Aktualność danych
W wielu zastosowaniach biznesowych istotne jest, aby dane były możliwie jak najbardziej aktualne.
Opóźnienia w przesyłaniu lub przetwarzaniu danych mogą prowadzić do podejmowania decyzji na podstawie nieaktualnych informacji.
Problem ten jest szczególnie ważny w systemach finansowych czy logistycznych.

== Zarządzanie metadanymi
W dużych systemach istotne jest nie tylko przechowywanie samych danych, ale również informacji opisujących ich pochodzenie, strukturę i sposób wykorzystania.
Brak odpowiedniego zarządzania metadanymi utrudnia odnajdywanie danych, ich analizę oraz ocenę jakości.
Może to prowadzić do nieporozumień i błędnych interpretacji wyników.
