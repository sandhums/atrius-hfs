//! FHIRPath `htmlChecks()` — Narrative XHTML subset (txt-1 / txt-2).
//!
//! Spec: when invoked on a single [xhtml](https://hl7.org/fhir/narrative.html#xhtml)
//! element, returns true if the [HTML usage rules](https://hl7.org/fhir/narrative.html#rules)
//! are met, and false if they are not. Empty on any other kind of element, or a
//! collection of more than one item.
//!
//! [FHIR-56303](https://jira.hl7.org/browse/FHIR-56303) also allows a `string`:
//! the contents are parsed as a `div` fragment and the function returns false
//! if the string is not valid HTML or fails the same rules.
//!
//! Both R4/R5 Narrative constraints `txt-1` (allowed tags/attributes) and
//! `txt-2` (some non-whitespace content) are expressed as `htmlChecks()`, so
//! this implementation checks **both** — matching the narrative rules the
//! function is defined to enforce, and R6's merged `txt-1`.

use helios_fhirpath_support::{EvaluationError, EvaluationResult};
use roxmltree::{Document, Node, NodeType};

/// Tags from HTML 4.0 chapters 7–11 (except 9.4 INS/DEL) and 15, plus `a` and
/// `img`, matching HAPI `FHIRPathEngine.checkHtmlNames` (block list).
const ALLOWED_TAGS: &[&str] = &[
    "a",
    "abbr",
    "acronym",
    "address",
    "area",
    "b",
    "bdo",
    "big",
    "blockquote",
    "br",
    "caption",
    "cite",
    "code",
    "col",
    "colgroup",
    "dd",
    "dfn",
    "div",
    "dl",
    "dt",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "img",
    "kbd",
    "li",
    "map",
    "ol",
    "p",
    "pre",
    "q",
    "samp",
    "small",
    "span",
    "strong",
    "sub",
    "sup",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "tt",
    "ul",
    "var",
];

/// Attributes allowed on any allowed element (HAPI global list, plus `xml:id`).
const GLOBAL_ATTRS: &[&str] = &[
    "abbr",
    "accesskey",
    "align",
    "axis",
    "char",
    "charoff",
    "class",
    "colspan",
    "dir",
    "headers",
    "id",
    "lang",
    "rowspan",
    "scope",
    "span",
    "style",
    "tabindex",
    "title",
    "valign",
    "width",
    "xml:id",
    "xml:lang",
    "xml:space",
];

/// `element.attribute` pairs allowed in addition to [`GLOBAL_ATTRS`].
const ELEMENT_ATTRS: &[&str] = &[
    "a.charset",
    "a.coords",
    "a.href",
    "a.hreflang",
    "a.name",
    "a.rel",
    "a.rev",
    "a.shape",
    "a.type",
    "area.alt",
    "area.coords",
    "area.href",
    "area.nohref",
    "area.shape",
    "blockquote.cite",
    "div.xmlns",
    "img.alt",
    "img.border",
    "img.height",
    "img.ismap",
    "img.longdesc",
    "img.src",
    "img.usemap",
    "img.width",
    "map.name",
    "pre.space",
    "q.cite",
    "table.border",
    "table.cellpadding",
    "table.cellspacing",
    "table.frame",
    "table.rules",
    "table.summary",
    "table.width",
    "td.nowrap",
];

/// `htmlChecks()` — allowed tags/attributes **and** non-whitespace content.
pub fn html_checks_function(
    invocation_base: &EvaluationResult,
    args: &[EvaluationResult],
) -> Result<EvaluationResult, EvaluationError> {
    html_checks_impl(invocation_base, args, true)
}

/// `htmlChecks2()` — HAPI's content-only half of R4/R5 `txt-2`.
pub fn html_checks2_function(
    invocation_base: &EvaluationResult,
    args: &[EvaluationResult],
) -> Result<EvaluationResult, EvaluationError> {
    html_checks_impl(invocation_base, args, false)
}

fn html_checks_impl(
    invocation_base: &EvaluationResult,
    args: &[EvaluationResult],
    check_names: bool,
) -> Result<EvaluationResult, EvaluationError> {
    if !args.is_empty() {
        return Err(EvaluationError::InvalidArity(
            "Function 'htmlChecks' expects 0 arguments".to_string(),
        ));
    }

    let item = match singleton(invocation_base) {
        Some(item) => item,
        None => return Ok(EvaluationResult::Empty),
    };

    let Some(xhtml) = xhtml_or_string(item) else {
        return Ok(EvaluationResult::Empty);
    };

    Ok(EvaluationResult::boolean(xhtml_is_ok(xhtml, check_names)))
}

fn singleton(result: &EvaluationResult) -> Option<&EvaluationResult> {
    match result {
        EvaluationResult::Empty => None,
        EvaluationResult::Collection { items, .. } => {
            if items.len() == 1 {
                Some(&items[0])
            } else {
                None
            }
        }
        other => Some(other),
    }
}

fn xhtml_or_string(item: &EvaluationResult) -> Option<&str> {
    match item {
        EvaluationResult::String(s, type_info, _) => {
            if let Some(ti) = type_info {
                let name = ti.name.as_str();
                if name.eq_ignore_ascii_case("xhtml")
                    || name.eq_ignore_ascii_case("string")
                    || name.eq_ignore_ascii_case("String")
                {
                    return Some(s.as_str());
                }
                // Other FHIR string-like primitives are still strings of markup
                // when they happen to hold XHTML; the spec only names xhtml and
                // string, so stay conservative.
                return None;
            }
            Some(s.as_str())
        }
        _ => None,
    }
}

fn xhtml_is_ok(raw: &str, check_names: bool) -> bool {
    let Some(xml) = xml_document_text(raw) else {
        return false;
    };
    let Ok(doc) = Document::parse(&xml) else {
        return false;
    };
    let root = doc.root_element();
    if check_names && !check_html_names(root) {
        return false;
    }
    has_non_whitespace_content(root)
}

/// Expand a few HTML named entities, then parse as XML, wrapping a fragment
/// in a `div` if needed. `None` if the text is not well-formed.
fn xml_document_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let expanded = expand_named_entities(trimmed);
    if Document::parse(expanded.as_ref()).is_ok() {
        return Some(expanded.into_owned());
    }
    let wrapped = format!("<div xmlns=\"http://www.w3.org/1999/xhtml\">{expanded}</div>");
    if Document::parse(&wrapped).is_ok() {
        Some(wrapped)
    } else {
        None
    }
}

fn expand_named_entities(input: &str) -> std::borrow::Cow<'_, str> {
    if !input.contains('&') {
        return std::borrow::Cow::Borrowed(input);
    }
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        if rest.starts_with("&#") {
            if let Some(semi) = rest.find(';') {
                out.push_str(&rest[..=semi]);
                rest = &rest[semi + 1..];
                continue;
            }
        }
        if let Some(semi) = rest.find(';') {
            let name = &rest[1..semi];
            if let Some(ch) = named_entity(name) {
                out.push(ch);
                rest = &rest[semi + 1..];
                continue;
            }
        }
        out.push('&');
        rest = &rest[1..];
    }
    out.push_str(rest);
    std::borrow::Cow::Owned(out)
}

fn named_entity(name: &str) -> Option<char> {
    Some(match name {
        "nbsp" => '\u{00A0}',
        "iexcl" => '¡',
        "cent" => '¢',
        "pound" => '£',
        "copy" => '©',
        "reg" => '®',
        "deg" => '°',
        "plusmn" => '±',
        "sup2" => '²',
        "sup3" => '³',
        "micro" => 'µ',
        "para" => '¶',
        "middot" => '·',
        "times" => '×',
        "divide" => '÷',
        "ndash" => '–',
        "mdash" => '—',
        "lsquo" => '‘',
        "rsquo" => '’',
        "ldquo" => '“',
        "rdquo" => '”',
        "bull" => '•',
        "hellip" => '…',
        "trade" => '™',
        // XML predefined — leave as-is if they reach here without a semicolon
        // path; with a semicolon they are already valid XML and are not expanded.
        _ => return None,
    })
}

fn check_html_names(node: Node<'_, '_>) -> bool {
    match node.node_type() {
        NodeType::Comment => !node
            .text()
            .is_some_and(|t| t.trim_start().starts_with("DOCTYPE")),
        NodeType::Element => {
            let tag = node.tag_name().name();
            if !is_allowed_tag(tag) {
                return false;
            }
            for attr in node.attributes() {
                let an = attribute_qname(attr);
                if !is_allowed_attr(tag, &an) {
                    return false;
                }
            }
            node.children().all(check_html_names)
        }
        _ => true,
    }
}

fn attribute_qname(attr: roxmltree::Attribute<'_, '_>) -> String {
    match attr.namespace() {
        Some("http://www.w3.org/XML/1998/namespace") => format!("xml:{}", attr.name()),
        Some("http://www.w3.org/2000/xmlns/") => {
            if attr.name() == "xmlns" {
                "xmlns".to_string()
            } else {
                format!("xmlns:{}", attr.name())
            }
        }
        _ if attr.name() == "xmlns" => "xmlns".to_string(),
        _ => attr.name().to_string(),
    }
}

fn is_allowed_tag(tag: &str) -> bool {
    ALLOWED_TAGS.binary_search(&tag).is_ok()
}

fn is_allowed_attr(tag: &str, attr: &str) -> bool {
    if attr.starts_with("xmlns") {
        return true;
    }
    if GLOBAL_ATTRS.binary_search(&attr).is_ok() {
        return true;
    }
    let pair = format!("{tag}.{attr}");
    ELEMENT_ATTRS.binary_search(&pair.as_str()).is_ok()
}

fn has_non_whitespace_content(node: Node<'_, '_>) -> bool {
    match node.node_type() {
        NodeType::Text | NodeType::Comment => node
            .text()
            .is_some_and(|t| t.chars().any(|c| !c.is_whitespace())),
        NodeType::Element if node.tag_name().name().eq_ignore_ascii_case("img") => true,
        NodeType::Element => node.children().any(has_non_whitespace_content),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xhtml(s: &str) -> EvaluationResult {
        EvaluationResult::fhir_string(s.to_string(), "xhtml")
    }

    fn string(s: &str) -> EvaluationResult {
        EvaluationResult::string(s.to_string())
    }

    fn checks(base: &EvaluationResult) -> EvaluationResult {
        html_checks_function(base, &[]).unwrap()
    }

    #[test]
    fn typical_narrative_div_passes() {
        let div = "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p>Jane Doe</p></div>";
        assert_eq!(checks(&xhtml(div)), EvaluationResult::boolean(true));
        assert_eq!(checks(&string(div)), EvaluationResult::boolean(true));
    }

    #[test]
    fn empty_div_fails_content_rule() {
        let div = "<div xmlns=\"http://www.w3.org/1999/xhtml\">  </div>";
        assert_eq!(checks(&xhtml(div)), EvaluationResult::boolean(false));
    }

    #[test]
    fn img_counts_as_content() {
        let div = "<div xmlns=\"http://www.w3.org/1999/xhtml\"><img src=\"photo.jpg\" alt=\"photo\"/></div>";
        assert_eq!(checks(&xhtml(div)), EvaluationResult::boolean(true));
    }

    #[test]
    fn script_is_rejected() {
        let div = "<div xmlns=\"http://www.w3.org/1999/xhtml\"><script>alert(1)</script>Hi</div>";
        assert_eq!(checks(&xhtml(div)), EvaluationResult::boolean(false));
    }

    #[test]
    fn onclick_is_rejected() {
        let div = "<div xmlns=\"http://www.w3.org/1999/xhtml\"><p onclick=\"x()\">Hi</p></div>";
        assert_eq!(checks(&xhtml(div)), EvaluationResult::boolean(false));
    }

    #[test]
    fn nbsp_entity_is_accepted() {
        let div = "<div xmlns=\"http://www.w3.org/1999/xhtml\">Hello&nbsp;world</div>";
        assert_eq!(checks(&xhtml(div)), EvaluationResult::boolean(true));
    }

    #[test]
    fn empty_or_wrong_type_is_empty() {
        assert_eq!(checks(&EvaluationResult::Empty), EvaluationResult::Empty);
        assert_eq!(
            checks(&EvaluationResult::integer(1)),
            EvaluationResult::Empty
        );
        assert_eq!(
            checks(&EvaluationResult::collection(vec![
                xhtml("<div xmlns=\"http://www.w3.org/1999/xhtml\">a</div>"),
                xhtml("<div xmlns=\"http://www.w3.org/1999/xhtml\">b</div>"),
            ])),
            EvaluationResult::Empty
        );
    }

    #[test]
    fn extra_argument_is_arity_error() {
        let err = html_checks_function(&xhtml("<div>x</div>"), &[EvaluationResult::boolean(true)])
            .unwrap_err();
        assert!(matches!(err, EvaluationError::InvalidArity(_)));
    }

    #[test]
    fn allowlists_are_sorted_for_binary_search() {
        fn sorted(items: &[&str]) -> bool {
            items.windows(2).all(|w| w[0] < w[1])
        }
        assert!(sorted(ALLOWED_TAGS));
        assert!(sorted(GLOBAL_ATTRS));
        assert!(sorted(ELEMENT_ATTRS));
    }
}
