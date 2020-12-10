use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn type_tree(attr: TokenStream, item: TokenStream) -> TokenStream {
    println!("{:?}", item);
    item
}