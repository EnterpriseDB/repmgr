# Make "html" the default target, since that is what most people tend
# to want to use.
html:

all: html

subdir = doc
repmgr_top_builddir = ..
include $(repmgr_top_builddir)/Makefile.global

XMLINCLUDE = --path .

ifndef XMLLINT
XMLLINT = $(missing) xmllint
endif

ifndef XSLTPROC
XSLTPROC = $(missing) xsltproc
endif

ifndef FOP
FOP = $(missing) fop
endif

override XSLTPROCFLAGS += --stringparam repmgr.version '$(REPMGR_VERSION)'

GENERATED_XML = version.xml
ALLXML := $(wildcard $(srcdir)/*.xml) $(GENERATED_XML)


version.xml: $(repmgr_top_builddir)/repmgr_version.h
	{ \
	  echo "<!ENTITY repmgrversion \"$(REPMGR_VERSION)\">"; \
	  echo "<!ENTITY releasedate \"$(REPMGR_RELEASE_DATE)\">"; \
	} > $@

##
## HTML
##


html: html-stamp

html-stamp: stylesheet.xsl repmgr.xml $(ALLXML)
	$(XMLLINT) $(XMLINCLUDE) --noout --valid $(word 2,$^)
	$(XSLTPROC) $(XMLINCLUDE) $(XSLTPROCFLAGS) $(XSLTPROC_HTML_FLAGS) $(wordlist 1,2,$^)
	cp $(srcdir)/stylesheet.css  $(srcdir)/website-docs.css html/
	touch $@

# single-page HTML
repmgr.html: stylesheet-html-nochunk.xsl repmgr.xml $(ALLXML)
	$(XMLLINT) $(XMLINCLUDE) --noout --valid $(word 2,$^)
	$(XSLTPROC) $(XMLINCLUDE) $(XSLTPROCFLAGS) $(XSLTPROC_HTML_FLAGS) -o $@ $(wordlist 1,2,$^)


zip: html
	cp -r html repmgr-docs-$(REPMGR_VERSION)
	zip -r repmgr-docs-$(REPMGR_VERSION).zip repmgr-docs-$(REPMGR_VERSION)
	rm -rf repmgr-docs-$(REPMGR_VERSION)

##
## Print
##

repmgr.pdf:
	$(error Invalid target; use repmgr-A4.pdf or repmgr-US.pdf as targets)

# Standard paper size

repmgr-A4.fo: stylesheet-fo.xsl repmgr.xml $(ALLXML)
	$(XMLLINT) $(XMLINCLUDE) --noout --valid $(word 2,$^)
	$(XSLTPROC) $(XMLINCLUDE) $(XSLTPROCFLAGS) --stringparam paper.type A4 -o $@ $(wordlist 1,2,$^)

repmgr-A4.pdf: repmgr-A4.fo
	$(FOP) -fo $< -pdf $@

# North American paper size

repmgr-US.fo: stylesheet-fo.xsl repmgr.xml $(ALLXML)
	$(XMLLINT) $(XMLINCLUDE) --noout --valid $(word 2,$^)
	$(XSLTPROC) $(XMLINCLUDE) $(XSLTPROCFLAGS) --stringparam paper.type USletter -o $@ $(wordlist 1,2,$^)

repmgr-US.pdf: repmgr-US.fo
	$(FOP) -fo $< -pdf $@


install: html
	@$(MKDIR_P) $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr
	@$(INSTALL_DATA) $(wildcard html/*.html) $(wildcard html/*.css) $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr
	@echo Installed docs to $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr

clean:
	rm -f html-stamp
	rm -f HTML.index $(GENERATED_XML)
	rm -f repmgr.html
	rm -f repmgr-A4.pdf
	rm -f repmgr-US.pdf
	rm -f html/*

maintainer-clean:
	rm -rf html

.PHONY: html
