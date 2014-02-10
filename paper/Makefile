main.pdf: *.tex 
	pdflatex main.tex
	bibtex main 
	pdflatex main.tex
	pdflatex main.tex


clean:
	-rm main.pdf
	-rm *.aux *.bbl *.blg *.log
	-rm *~
