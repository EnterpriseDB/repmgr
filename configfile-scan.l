/*
 * Scanner for the configuration file
 */

%{

#include <setjmp.h>

#include "repmgr.h"
#include "configfile.h"

/*
 * flex emits a yy_fatal_error() function that it calls in response to
 * critical errors like malloc failure, file I/O errors, and detection of
 * internal inconsistency.  That function prints a message and calls exit().
 * Mutate it to instead call our handler, which jumps out of the parser.
 */
#undef fprintf
#define fprintf(file, fmt, msg) CONF_flex_fatal(msg)

enum
{
	CONF_ID = 1,
	CONF_STRING = 2,
	CONF_INTEGER = 3,
	CONF_REAL = 4,
	CONF_EQUALS = 5,
	CONF_UNQUOTED_STRING = 6,
	CONF_QUALIFIED_ID = 7,
	CONF_EOL = 99,
	CONF_ERROR = 100
};

static unsigned int ConfigFileLineno;
static const char *CONF_flex_fatal_errmsg;
static sigjmp_buf *CONF_flex_fatal_jmp;

static char *CONF_scanstr(const char *s);
static int	CONF_flex_fatal(const char *msg);

static bool ProcessConfigFile(FILE *fp, const char *config_file, KeyValueList *contents, t_configuration_options *options, ItemList *error_list, ItemList *warning_list);

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="CONF_yy"


SIGN			("-"|"+")
DIGIT			[0-9]
HEXDIGIT		[0-9a-fA-F]

UNIT_LETTER		[a-zA-Z]

INTEGER			{SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*

EXPONENT		[Ee]{SIGN}?{DIGIT}+
REAL			{SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?

LETTER			[A-Za-z_\200-\377]
LETTER_OR_DIGIT [A-Za-z_0-9\200-\377]

ID				{LETTER}{LETTER_OR_DIGIT}*
QUALIFIED_ID	{ID}"."{ID}

UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/])*
STRING			\'([^'\\\n]|\\.|\'\')*\'

%%

\n				ConfigFileLineno++; return CONF_EOL;
[ \t\r]+		/* eat whitespace */
#.*				/* eat comment (.* matches anything until newline) */

{ID}			return CONF_ID;
{QUALIFIED_ID}	return CONF_QUALIFIED_ID;
{STRING}		return CONF_STRING;
{UNQUOTED_STRING} return CONF_UNQUOTED_STRING;
{INTEGER}		return CONF_INTEGER;
{REAL}			return CONF_REAL;
=				return CONF_EQUALS;

.				return CONF_ERROR;

%%

extern bool
ProcessRepmgrConfigFile(FILE *fp, const char *config_file, t_configuration_options *options, ItemList *error_list, ItemList *warning_list)
{
	return ProcessConfigFile(fp, config_file, NULL, options, error_list, warning_list);
}

extern bool
ProcessPostgresConfigFile(FILE *fp, const char *config_file, KeyValueList *contents, ItemList *error_list, ItemList *warning_list)
{
	return ProcessConfigFile(fp, config_file, contents, NULL, error_list, warning_list);
}

static bool
ProcessConfigFile(FILE *fp, const char *config_file, KeyValueList *contents, t_configuration_options *options, ItemList *error_list, ItemList *warning_list)
{
	volatile bool OK = true;
	volatile YY_BUFFER_STATE lex_buffer = NULL;
	sigjmp_buf	flex_fatal_jmp;
	int			errorcount;
	int			token;

	if (sigsetjmp(flex_fatal_jmp, 1) == 0)
	{
		CONF_flex_fatal_jmp = &flex_fatal_jmp;
	}
	else
	{
		/*
		 * Regain control after a fatal, internal flex error.  It may have
		 * corrupted parser state.  Consequently, abandon the file, but trust
		 * that the state remains sane enough for yy_delete_buffer().
		 */
		item_list_append_format(error_list,
								"%s at file \"%s\" line %u",
								CONF_flex_fatal_errmsg, config_file, ConfigFileLineno);
		OK = false;
		goto cleanup;
	}

	/*
	 * Parse
	 */
	ConfigFileLineno = 1;
	errorcount = 0;

	lex_buffer = yy_create_buffer(fp, YY_BUF_SIZE);
	yy_switch_to_buffer(lex_buffer);

	/* This loop iterates once per logical line */
	while ((token = yylex()))
	{
		char	   *opt_name = NULL;
		char	   *opt_value = NULL;

		if (token == CONF_EOL)	/* empty or comment line */
			continue;

		/* first token on line is option name */
		if (token != CONF_ID && token != CONF_QUALIFIED_ID)
			goto parse_error;
		opt_name = pstrdup(yytext);

		/* next we have an optional equal sign; discard if present */
		token = yylex();
		if (token == CONF_EQUALS)
			token = yylex();

		/* now we must have the option value */
		if (token != CONF_ID &&
			token != CONF_STRING &&
			token != CONF_INTEGER &&
			token != CONF_REAL &&
			token != CONF_UNQUOTED_STRING)
			goto parse_error;
		if (token == CONF_STRING)	/* strip quotes and escapes */
			opt_value = CONF_scanstr(yytext);
		else
			opt_value = pstrdup(yytext);

		/* now we'd like an end of line, or possibly EOF */
		token = yylex();
		if (token != CONF_EOL)
		{
			if (token != 0)
				goto parse_error;
			/* treat EOF like \n for line numbering purposes, cf bug 4752 */
			ConfigFileLineno++;
		}

		/* OK, process the option name and value */
		if (contents != NULL)
		{
			key_value_list_replace_or_set(contents,
										  opt_name,
										  opt_value);
		}

		if (options != NULL)
		{
			parse_configuration_item(options,
									 error_list,
									 warning_list,
									 opt_name,
									 opt_value);
		}

		/* break out of loop if read EOF, else loop for next line */
		if (token == 0)
			break;
		continue;

parse_error:
		/* release storage if we allocated any on this line */
		if (opt_name)
			pfree(opt_name);
		if (opt_value)
			pfree(opt_value);

		/* report the error */
		if (token == CONF_EOL || token == 0)
		{
			item_list_append_format(error_list,
									_("syntax error in file \"%s\" line %u, near end of line"),
								    config_file, ConfigFileLineno - 1);
		}
		else
		{
			item_list_append_format(error_list,
									_("syntax error in file \"%s\" line %u, near token \"%s\""),
									config_file, ConfigFileLineno, yytext);
		}
		OK = false;
		errorcount++;

		/*
		 * To avoid producing too much noise when fed a totally bogus file,
		 * give up after 100 syntax errors per file (an arbitrary number).
		 * Also, if we're only logging the errors at DEBUG level anyway, might
		 * as well give up immediately.  (This prevents postmaster children
		 * from bloating the logs with duplicate complaints.)
		 */
		if (errorcount >= 100)
		{
			fprintf(stderr,
 				   _("too many syntax errors found, abandoning file \"%s\"\n"),
				   config_file);
			break;
		}

		/* resync to next end-of-line or EOF */
		while (token != CONF_EOL && token != 0)
			token = yylex();
		/* break out of loop on EOF */
		if (token == 0)
			break;
	}

cleanup:
	yy_delete_buffer(lex_buffer);

	return OK;
}


/*
 *		scanstr
 *
 * Strip the quotes surrounding the given string, and collapse any embedded
 * '' sequences and backslash escapes.
 *
 * the string returned is palloc'd and should eventually be pfree'd by the
 * caller.
 */
static char *
CONF_scanstr(const char *s)
{
	char	   *newStr;
	int			len,
				i,
				j;

	Assert(s != NULL && s[0] == '\'');
	len = strlen(s);
	Assert(s != NULL);

	Assert(len >= 2);
	Assert(s[len - 1] == '\'');

	/* Skip the leading quote; we'll handle the trailing quote below */
	s++, len--;

	/* Since len still includes trailing quote, this is enough space */
	newStr = palloc(len);

	for (i = 0, j = 0; i < len; i++)
	{
		if (s[i] == '\\')
		{
			i++;
			switch (s[i])
			{
				case 'b':
					newStr[j] = '\b';
					break;
				case 'f':
					newStr[j] = '\f';
					break;
				case 'n':
					newStr[j] = '\n';
					break;
				case 'r':
					newStr[j] = '\r';
					break;
				case 't':
					newStr[j] = '\t';
					break;
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
					{
						int			k;
						long		octVal = 0;

						for (k = 0;
							 s[i + k] >= '0' && s[i + k] <= '7' && k < 3;
							 k++)
							octVal = (octVal << 3) + (s[i + k] - '0');
						i += k - 1;
						newStr[j] = ((char) octVal);
					}
					break;
				default:
					newStr[j] = s[i];
					break;
			}					/* switch */
		}
		else if (s[i] == '\'' && s[i + 1] == '\'')
		{
			/* doubled quote becomes just one quote */
			newStr[j] = s[++i];
		}
		else
			newStr[j] = s[i];
		j++;
	}

	/* We copied the ending quote to newStr, so replace with \0 */
	Assert(j > 0 && j <= len);
	newStr[--j] = '\0';

	return newStr;
}


/*
 * Flex fatal errors bring us here.  Stash the error message and jump back to
 * ParseConfigFp().  Assume all msg arguments point to string constants; this
 * holds for flex 2.5.31 (earliest we support) and flex 2.5.35 (latest as of
 * this writing).  Otherwise, we would need to copy the message.
 *
 * We return "int" since this takes the place of calls to fprintf().
*/
static int
CONF_flex_fatal(const char *msg)
{
	CONF_flex_fatal_errmsg = msg;
	siglongjmp(*CONF_flex_fatal_jmp, 1);
	return 0;					/* keep compiler quiet */
}
