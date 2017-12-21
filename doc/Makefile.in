repmgr_subdir = doc
repmgr_top_builddir = ..
include $(repmgr_top_builddir)/Makefile.global

ifndef JADE
JADE = $(missing) jade
endif

SGMLINCLUDE = -D . -D ${srcdir}

SPFLAGS += -wall -wno-unused-param -wno-empty -wfully-tagged

JADE.html.call = $(JADE) $(JADEFLAGS) $(SPFLAGS) $(SGMLINCLUDE) $(CATALOG) -t sgml -i output-html

ALLSGML := $(wildcard $(srcdir)/*.sgml)
# to build bookindex
ALMOSTALLSGML := $(filter-out %bookindex.sgml,$(ALLSGML))
GENERATED_SGML = version.sgml bookindex.sgml

Makefile: Makefile.in
	cd $(repmgr_top_builddir) && ./config.status doc/Makefile

all: html

html: html-stamp

html-stamp: repmgr.sgml $(ALLSGML) $(GENERATED_SGML) stylesheet.dsl website-docs.css
	$(MKDIR_P) html
	$(JADE.html.call) -d stylesheet.dsl -i include-index $<
	cp $(srcdir)/stylesheet.css $(srcdir)/website-docs.css html/
	touch $@

repmgr.html: repmgr.sgml $(ALLSGML) $(GENERATED_SGML) stylesheet.dsl website-docs.css
	sed '/html-index-filename/a\
(define nochunks  #t)' <stylesheet.dsl >nochunks.dsl
	$(JADE.html.call) -d nochunks.dsl -i include-index $< >repmgr.html

version.sgml: ${repmgr_top_builddir}/repmgr_version.h
	{ \
	  echo "<!ENTITY repmgrversion \"$(REPMGR_VERSION)\">"; \
	} > $@

HTML.index: repmgr.sgml $(ALMOSTALLSGML) stylesheet.dsl
	@$(MKDIR_P) html
	$(JADE.html.call) -d stylesheet.dsl -V html-index $<

website-docs.css:
	@$(MKDIR_P) html
	curl http://www.postgresql.org/media/css/docs.css > ${srcdir}/website-docs.css

bookindex.sgml: HTML.index
ifdef COLLATEINDEX
	LC_ALL=C $(PERL) $(COLLATEINDEX) -f -g -i 'bookindex' -o $@ $<
else
	@$(missing) collateindex.pl $< $@
endif

clean:
	rm -f html-stamp
	rm -f HTML.index $(GENERATED_SGML)

maintainer-clean:
	rm -rf html
	rm -rf Makefile

zip: html
	cp -r html repmgr-docs-$(REPMGR_VERSION)
	zip -r repmgr-docs-$(REPMGR_VERSION).zip repmgr-docs-$(REPMGR_VERSION)
	rm -rf repmgr-docs-$(REPMGR_VERSION)

install: html
	@$(MKDIR_P) $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr
	@$(INSTALL_DATA) $(wildcard html/*.html) $(wildcard html/*.css) $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr
	@echo Installed docs to $(DESTDIR)$(docdir)/$(docmoduledir)/repmgr

.PHONY: html all
