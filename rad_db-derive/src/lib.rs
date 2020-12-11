use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn type_tree(_attr: TokenStream, item: TokenStream) -> TokenStream {
    println!("{:?}", item);
    item
}