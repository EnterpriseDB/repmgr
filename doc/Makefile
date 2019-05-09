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

override XSLTPROCFLAGS += --stringparam repmgr.version '$(REPMGR_VERSION)'

ALLSGML := $(wildcard $(srcdir)/*.sgml)
GENERATED_SGML = version.sgml

##
## HTML
##


html: html-stamp

html-stamp: stylesheet.xsl repmgr.sgml $(ALLSGML) $(GENERATED_SGML)
	$(XMLLINT) $(XMLINCLUDE) --noout --valid $(word 2,$^)
	$(XSLTPROC) $(XMLINCLUDE) $(XSLTPROCFLAGS) $(XSLTPROC_HTML_FLAGS) $(wordlist 1,2,$^)
	cp $(srcdir)/stylesheet.css html/
	touch $@

# single-page HTML
repmgr.html: stylesheet-html-nochunk.xsl repmgr.sgml $(ALLSGML) $(GENERATED_SGML)
	$(XMLLINT) $(XMLINCLUDE) --noout --valid $(word 2,$^)
	$(XSLTPROC) $(XMLINCLUDE) $(XSLTPROCFLAGS) $(XSLTPROC_HTML_FLAGS) -o $@ $(wordlist 1,2,$^)

version.sgml: $(repmgr_top_builddir)/repmgr_version.h
	{ \
	  echo "<!ENTITY repmgrversion \"$(REPMGR_VERSION)\">"; \
	} > $@

zip: html
	cp -r html repmgr-docs-$(REPMGR_VERSION)
	zip -r repmgr-docs-$(REPMGR_VERSION).zip repmgr-docs-$(REPMGR_VERSION)
	rm -rf repmgr-docs-$(REPMGR_VERSION)

install: html
	@$(MKDIR_P) $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr
	@$(INSTALL_DATA) $(wildcard html/*.html) $(wildcard html/*.css) $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr
	@echo Installed docs to $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr

clean:
	rm -f html-stamp
	rm -f HTML.index $(GENERATED_SGML)

maintainer-clean:
	rm -rf html

.PHONY: html
