//! Debug trace support for step-by-step FHIRPath evaluation tracing.
//!
//! When enabled via `FHIRPATH_DEBUG_TRACE=1`, this module records every intermediate
//! evaluation step with position, length, and function name from the source expression.

use crate::parser::{
    ExprSpan, Expression, SpannedExprKind, SpannedExpression, SpannedInvocation, SpannedTerm,
};
use helios_fhirpath_support::EvaluationResult;
use std::collections::HashMap;

/// A single step in the debug trace, recording what was evaluated and the result.
#[derive(Debug, Clone)]
pub struct DebugTraceStep {
    pub position: usize,
    pub length: usize,
    pub function_name: String,
    pub result: EvaluationResult,
}

/// Collects debug trace steps during evaluation.
#[derive(Debug)]
pub struct DebugTracer {
    pub steps: Vec<DebugTraceStep>,
    pub span_map: HashMap<*const Expression, ExprSpan>,
}

// SAFETY: The raw pointers in span_map are only used as lookup keys during
// evaluation while the Expression tree they point into is alive and immutable.
// They are never dereferenced through the DebugTracer.
unsafe impl Send for DebugTracer {}
unsafe impl Sync for DebugTracer {}

impl DebugTracer {
    pub fn new(span_map: HashMap<*const Expression, ExprSpan>) -> Self {
        Self {
            steps: Vec::new(),
            span_map,
        }
    }

    /// Record a trace step if we have span info for this expression.
    pub fn record(&mut self, expr: &Expression, result: &EvaluationResult) {
        let ptr = expr as *const Expression;
        if let Some(span) = self.span_map.get(&ptr) {
            self.steps.push(DebugTraceStep {
                position: span.position,
                length: span.length,
                function_name: expression_debug_name(expr),
                result: result.clone(),
            });
        }
    }
}

/// Build a map from Expression pointer addresses to their spans.
///
/// This walks the SpannedExpression and Expression trees in parallel (they have
/// identical structure since Expression was produced by `to_expression()`) and
/// records the mapping from each `&Expression` address to its span.
pub fn build_span_map(
    spanned: &SpannedExpression,
    expr: &Expression,
) -> HashMap<*const Expression, ExprSpan> {
    let mut map = HashMap::new();
    build_span_map_inner(spanned, expr, &mut map);
    map
}

fn build_span_map_inner(
    spanned: &SpannedExpression,
    expr: &Expression,
    map: &mut HashMap<*const Expression, ExprSpan>,
) {
    // Map this expression's pointer to its span
    map.insert(expr as *const Expression, spanned.span.clone());

    // Recursively map children
    match (&spanned.kind, expr) {
        (SpannedExprKind::Term(st), Expression::Term(t)) => match (st, t) {
            (SpannedTerm::Parenthesized(se), crate::parser::Term::Parenthesized(e)) => {
                build_span_map_inner(se, e, map);
            }
            (
                SpannedTerm::Invocation(SpannedInvocation::Function(_, sargs)),
                crate::parser::Term::Invocation(crate::parser::Invocation::Function(_, args)),
            ) => {
                for (sa, a) in sargs.iter().zip(args.iter()) {
                    build_span_map_inner(sa, a, map);
                }
            }
            _ => {}
        },
        (SpannedExprKind::Invocation(sb, sinv), Expression::Invocation(eb, _einv)) => {
            build_span_map_inner(sb, eb, map);
            // Map function args in the invocation
            if let (
                SpannedInvocation::Function(_, sargs),
                crate::parser::Invocation::Function(_, args),
            ) = (sinv, _einv)
            {
                for (sa, a) in sargs.iter().zip(args.iter()) {
                    build_span_map_inner(sa, a, map);
                }
            }
        }
        (SpannedExprKind::Indexer(se, si), Expression::Indexer(ee, ei)) => {
            build_span_map_inner(se, ee, map);
            build_span_map_inner(si, ei, map);
        }
        (SpannedExprKind::Polarity(_, se), Expression::Polarity(_, ee)) => {
            build_span_map_inner(se, ee, map);
        }
        (SpannedExprKind::Multiplicative(sl, _, sr), Expression::Multiplicative(el, _, er))
        | (SpannedExprKind::Additive(sl, _, sr), Expression::Additive(el, _, er))
        | (SpannedExprKind::Inequality(sl, _, sr), Expression::Inequality(el, _, er))
        | (SpannedExprKind::Equality(sl, _, sr), Expression::Equality(el, _, er))
        | (SpannedExprKind::Membership(sl, _, sr), Expression::Membership(el, _, er))
        | (SpannedExprKind::Or(sl, _, sr), Expression::Or(el, _, er)) => {
            build_span_map_inner(sl, el, map);
            build_span_map_inner(sr, er, map);
        }
        (SpannedExprKind::Type(se, _, _), Expression::Type(ee, _, _)) => {
            build_span_map_inner(se, ee, map);
        }
        (SpannedExprKind::Union(sl, sr), Expression::Union(el, er))
        | (SpannedExprKind::And(sl, sr), Expression::And(el, er))
        | (SpannedExprKind::Implies(sl, sr), Expression::Implies(el, er)) => {
            build_span_map_inner(sl, el, map);
            build_span_map_inner(sr, er, map);
        }
        (SpannedExprKind::Lambda(_, se), Expression::Lambda(_, ee)) => {
            build_span_map_inner(se, ee, map);
        }
        _ => {}
    }
}

/// Return a human-readable debug name for an expression variant.
fn expression_debug_name(expr: &Expression) -> String {
    match expr {
        Expression::Term(t) => match t {
            crate::parser::Term::Literal(lit) => format!("{}", lit),
            crate::parser::Term::Invocation(inv) => match inv {
                crate::parser::Invocation::Member(name) => name.clone(),
                crate::parser::Invocation::Function(name, _) => format!("{}()", name),
                crate::parser::Invocation::This => "$this".to_string(),
                crate::parser::Invocation::Index => "$index".to_string(),
                crate::parser::Invocation::Total => "$total".to_string(),
            },
            crate::parser::Term::ExternalConstant(name) => format!("%{}", name),
            crate::parser::Term::Parenthesized(_) => "()".to_string(),
        },
        Expression::Invocation(_, inv) => match inv {
            crate::parser::Invocation::Member(name) => name.clone(),
            crate::parser::Invocation::Function(name, _) => format!("{}()", name),
            crate::parser::Invocation::This => "$this".to_string(),
            crate::parser::Invocation::Index => "$index".to_string(),
            crate::parser::Invocation::Total => "$total".to_string(),
        },
        Expression::Indexer(_, _) => "[]".to_string(),
        Expression::Polarity(op, _) => format!("unary {}", op),
        Expression::Multiplicative(_, op, _) => op.clone(),
        Expression::Additive(_, op, _) => op.clone(),
        Expression::Type(_, op, _) => op.clone(),
        Expression::Union(_, _) => "|".to_string(),
        Expression::Inequality(_, op, _) => op.clone(),
        Expression::Equality(_, op, _) => op.clone(),
        Expression::Membership(_, op, _) => op.clone(),
        Expression::And(_, _) => "and".to_string(),
        Expression::Or(_, op, _) => op.clone(),
        Expression::Implies(_, _) => "implies".to_string(),
        Expression::Lambda(_, _) => "=>".to_string(),
        Expression::InstanceSelector(type_name, _) => format!("{} {{...}}", type_name),
    }
}
