import textwrap
from typing import Protocol


class DescriptionProvider(Protocol):
    @property
    def describer(self) -> str: ...
    @property
    def model_id(self) -> str: ...

    def describe(self, text: str, context: str) -> str:
        """Describe the given text according to the context.

        This uses a default prompt meant to generate a human readable description of the given text.
        """
        ...

    def prompt_for_description(self, prompt: str) -> str:
        """Prompt the LLM with the given prompt.

        Use this method if you want to use a custom prompt to describe your content.
        """
        ...

    @staticmethod
    def default_description_prompt(text: str, context: str) -> str:
        base = """
            You are a helpful assistant.

            I will give you some TEXT and CONTEXT.
            Write a concise, human-readable description of the TEXT suitable for displaying in a UI.
            - 1-2 sentences
            - Be factual and avoid speculation
            - No markdown
            - No preambles or labels, just the description itself.
            - Your entire reply MUST be only the description itself. No extra commentary.

            CONTEXT:
            {context}

            TEXT:
            {text}
            """

        return textwrap.dedent(base).format(context=context, text=text).strip()
